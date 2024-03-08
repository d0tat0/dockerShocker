import os
import sys
import json
import requests
import shlex
import socket
import glob
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.subprocess import SubprocessHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.models.variable import Variable


class Dagger:
    def __init__(self):

        dags_deploy_dir = os.path.dirname(os.path.realpath(__file__))
        deploy_alias = dags_deploy_dir.split("/")[-1]
        print(f"Dags Deploy Dir: {dags_deploy_dir}")
        print(f"Deploy Alias: {deploy_alias}")

        dags_config_path = os.path.join(dags_deploy_dir, "dags_config.json")
        self.dags_config = json.loads(open(dags_config_path, 'r').read())

        env_config_file_name = "env_config.json"
        env_config_path = os.path.join(os.path.dirname(dags_config_path), env_config_file_name)
        self.env_config = json.loads(open(env_config_path, 'r').read())
        self.env = socket.gethostname()

        dag_args = self.dags_config["dag_args"]
        dag_args["start_date"] = days_ago(1)
        self.default_dag_args = dag_args
        self.dag_prefix = deploy_alias

        self.source_override = Variable.get(f"{self.dag_prefix}_SOURCE", None)
        self.skip_if_exists_override = Variable.get(f"{self.dag_prefix}_SKIP_IF_EXISTS", None)

        self.skip_if_exists_value = "True"
        if self.skip_if_exists_override and self.skip_if_exists_override.lower() in ['f', 'false']:
            self.skip_if_exists_value = "False"

        self.enable_alert = True  # Variable.get(f"{self.dag_prefix}_ENABLE_ALERT", "False").lower() == "true"
        print(f"Enable Alert: {self.enable_alert}")

        self.deploy_location = os.path.join(self.env_config["deploy_location"], deploy_alias)
        file_name = os.path.join(self.deploy_location, 'config', 'harmonisation_config.json')
        self.harmonisation_config = json.loads(open(file_name).read())

        self.script_location = os.path.join(self.deploy_location, "scripts")
        self.venv_location = self.env_config.get("venv_location")
        if self.venv_location:
            self.venv_location = os.path.join(self.deploy_location, self.venv_location)
        self.base_config_path = os.path.join(self.deploy_location, "config/config.json")
        self.base_config = json.loads(open(self.base_config_path).read())
        self.ees_base_dir = self.harmonisation_config["resource_base_location"]

        self.run_as_user = self.env_config.get("run_as_user", "spark")
        self.spark_args = self.env_config.get("spark_args")
        self.jars = self.env_config.get("jars")
        self.spark_script = self.env_config.get("spark_bin_location")

        pager_config = self.env_config.get("pager_duty", {})
        self.pager_url = pager_config.get("url", None)
        self.pager_key = pager_config.get("routing_key", None)
        self._is_yarn_cluster = self.env_config.get("is_yarn_cluster", False)

        self.is_yarn_cluster_mode = False
        for arg, val in self.spark_args.items():
            if arg == "deploy-mode":
                if val == "cluster":
                    self.is_yarn_cluster_mode = True
                    break

    def run(self):
        for dag in self.dags_config["dags"]:
            if self.is_disabled(dag):
                continue
            if self.dag_prefix:
                dag["dag_id"] = self.dag_prefix + "_" + dag["dag_id"]
            globals()[dag["dag_id"]] = self.create_dag(dag)

    def is_disabled(self, obj):
        if obj.get("disabled"):
            return True
        return False

    def create_dag(self, dag_data):
        dag_name = dag_data["dag_id"]
        dag = DAG(
            dag_name,
            default_args=self.default_dag_args,
            description=dag_data.get("description", dag_name),
            schedule_interval=dag_data.get("schedule"),
            max_active_runs=1,
            tags=[self.dag_prefix],
            on_success_callback=self.notify_success,
            on_failure_callback=self.notify_failure
        )
        self.chain_tasks(dag_data, dag)
        return dag

    def chain_tasks(self, dag_data, dag, task_group=None):
        prev_task = None
        for task in dag_data.get("tasks", []):

            if self.is_disabled(task):
                continue

            curr_task = None
            self.init_config(task)
            is_extraction_job = task.get("is_extraction_job", False)

            if task.get('operator') == 'TaskGroupOperator':
                curr_task = self.chain_dag_in_task_group(task, dag, dag_data)
                if not curr_task:
                    continue

            elif is_extraction_job:
                curr_task = self.create_ees_command(task, task_group, dag)

            else:
                curr_task = self.create_orch_command(task, task_group, dag)

            if prev_task:
                prev_task.set_downstream(curr_task)
            prev_task = curr_task

    def chain_dag_in_task_group(self, task, dag, dag_data):
        if task["task_id"] != dag_data["dag_id"]:
            for sub_dag_data in self.dags_config["dags"]:
                if sub_dag_data["dag_id"] == task["task_id"]:
                    tg_task = TaskGroup(group_id=task["task_id"], dag=dag)
                    self.chain_tasks(sub_dag_data, dag, tg_task)
                    return tg_task
        print(f"TaskGroupOperator {task['task_id']} not found")

    def create_orch_command(self, task, task_group, dag):
        if task.get('operator') == "SparkOperator":
            task_command = self.create_spark_command(task)
        else:
            task_command = self.create_command(task)

        python_paths = [self.script_location,os.path.join(self.script_location,'data_reader')]
        venv_command = "source {}/bin/activate".format(task.get("virtual_env_location", self.venv_location))
        python_path_command = "export PYTHONPATH={}".format(":".join(python_paths))
        bash_command = "{} && {} && {}".format(venv_command, python_path_command, task_command)

        return PythonOperator(
            task_id=task["task_id"],
            dag=dag,
            python_callable=self.run_subprocess,
            op_kwargs={'task_id': task["task_id"], 'bash_command': bash_command},
            task_group=task_group,
            trigger_rule="none_failed_min_one_success"
        )

    def create_ees_command(self, task, task_group, dag):
        yarn_task = (task.get("cluster_job", False) or task.get("is_extraction_job", False)) and self._is_yarn_cluster
        ees_id = task.get("ees_id", None)
        ees_dict = None
        job_name = None

        for key in self.harmonisation_config["jobs"]:
            if str(self.harmonisation_config['jobs'][key]['id']) == str(ees_id):
                ees_dict = self.harmonisation_config['jobs'][key]
                job_name = key

        if ees_dict is None:
            raise Exception

        ees_tag = ees_dict['tag']
        if ees_tag is None or len(ees_tag) == 0 or not isinstance(ees_tag, str):
            print(ees_tag)
            raise Exception
        task_script_location = os.path.join(self.ees_base_dir,
                                            self.harmonisation_config["clone_location"],
                                            job_name,
                                            ees_tag)
        python_paths = [task_script_location, self.script_location]

        if task.get('additional_paths'):
            # script_location = os.path.dirname(task_script_location)
            for path in task.get('additional_paths'):
                python_paths.append(os.path.join(task_script_location, path))

        # setting up data path in op/kwargs
        d = task.get("op_kwargs", {})
        if "resources-folder" not in d and self.harmonisation_config["jobs"][job_name].get("resources_cloud_path"):
            d['resources-folder'] = os.path.join(self.harmonisation_config["resource_cloud_location"],
                                                 self.harmonisation_config["data_location"], job_name)
                                                 #self.harmonisation_config["jobs"][job_name][
                                                  #   "resources_cloud_path"].split('/')[-1])
            task["op_kwargs"] = d

        if task.get('operator') == "SparkOperator":
            task_command = self.create_spark_command(task, task_script_location, job_name)
        else:
            raise NotImplementedError

        ees_venv_location = os.path.join(self.harmonisation_config['resource_base_location'],
                                         self.harmonisation_config["venv_location"],
                                         job_name)

        venv_command = "source {}/bin/activate".format(ees_venv_location)
        python_path_command = "export PYTHONPATH={}".format(":".join(python_paths))

        if yarn_task:
            bash_command = "{} && {} && {}".format(venv_command,
                                                   python_path_command, task_command)
        else:
            _python_path = os.path.join(ees_venv_location, "bin", "python3")
            pyspark_python_command = f"export PYSPARK_PYTHON={_python_path}"
            pyspark_driver_python_command = f"export PYSPARK_DRIVER_PYTHON={_python_path}"
            bash_command = "{} && {} && {} && {} && {}".format(venv_command,
                                                               python_path_command,
                                                               pyspark_driver_python_command,
                                                               pyspark_python_command,
                                                               task_command)

        return PythonOperator(
            task_id=task["task_id"],
            dag=dag,
            python_callable=self.run_subprocess,
            op_kwargs={'task_id': task["task_id"], 'bash_command': bash_command},
            task_group=task_group,
            trigger_rule="none_failed_min_one_success"
        )

    def create_spark_command(self, task, ees_script_location=None, job_name=None):
        yarn_task = (task.get("cluster_job", False) or task.get("is_extraction_job", False)) and self._is_yarn_cluster
        spark_args = self.adjust_spark_conf(task)
        spark_command_lst = [self.spark_script]
        for arg, val in spark_args.items():
            if arg == "conf":
                for conf in val:
                    spark_command_lst.append(f"--conf {conf}")
            else:
                if arg == "deploy-mode" and yarn_task:
                    val = "cluster"
                spark_command_lst.append(f"--{arg} {val}")

        if yarn_task:
            files = [f"{self.deploy_location}/config/config.json",
                     f"{self.deploy_location}/config/harmonisation_config.json"]
            if task.get("is_extraction_job", False):  # make one more if condition for not extration job
                venv_location = os.path.join(self.ees_base_dir, self.harmonisation_config["venv_location"],
                                             job_name)
                tag_name = self.harmonisation_config['jobs'][job_name]['tag']
                tag_location = os.path.join(self.harmonisation_config['resource_base_location'],
                                            self.harmonisation_config['clone_location'], job_name, tag_name)
                code_zip = f"{tag_location}/{tag_name}.zip"
                for key in task["op_kwargs"]:
                    if key == "job-config":
                        files.append(f"{tag_location}/{task['op_kwargs'][key]}")

                for _config in task.get("additional_configs", []):
                    files.append(f"{tag_location}/{_config}")
            else:
                # Zipping process now happens in deploy.py with -VR argument
                venv_location = self.env_config["venv_location"]
                code_zip = f"{self.deploy_location}/scripts/scripts.zip"

            venv_folder = venv_location.split('/')[-1]
            spark_command_lst.append(
                f"--conf spark.executorEnv.PYSPARK_PYTHON=./{venv_folder}.zip/{venv_folder}/bin/python")
            spark_command_lst.append(
                f"--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./{venv_folder}.zip/{venv_folder}/bin/python")
            spark_command_lst.append(f"--conf spark.yarn.am.waitTime=5000s")

            spark_command_lst.append(f"--py-files {code_zip}")

            config_files = task.get("config_files", [])
            if len(config_files) > 0:
                file_params = [os.path.join(self.deploy_location, file) for file in config_files]
                files.extend(file_params)

            files = ",".join(files)

            spark_command_lst.append(f"--files {files}")
            spark_command_lst.append(f"--archives {venv_location}.zip")

        spark_command_lst.extend(["--jars {}".format(",".join(self.jars))])
        spark_command_lst.append(task['script_name'])

        # TODO See how it will be handled for other jobs
        if self._is_yarn_cluster and task.get('script_location', '').startswith('omop'):
            spark_command_lst.append("--is_yarn_cluster_mode 1")

        op_kwargs = task.get("op_kwargs", {})
        if self.dag_prefix.endswith("_test") and not task.get("is_extraction_job", False):
            op_kwargs['skip-if-exists'] = "False"
        if yarn_task and task.get("is_extraction_job", False):
            spark_command_lst.extend([f" --{k} {v.split('/')[-1]}" if k != "resources-folder" else f" --{k} {v}" for k, v in op_kwargs.items()]) #only take last og v for extraction
        else:
            spark_command_lst.extend([f" --{k} {v}" for k, v in op_kwargs.items()])
        script_location = self.script_location
        spark_command = " ".join(spark_command_lst)

        if task.get("is_extraction_job", False):
            bash_command = "cd {} && {} ".format(ees_script_location, spark_command)
        else:
            bash_command = "cd {} && {} ".format(os.path.join(script_location, task['script_location']), spark_command)
        return bash_command

    def adjust_spark_conf(self, task):
        spark_args = dict(self.spark_args)
        # task specific additional spark args needed for EES
        task_specific_spark_args = task.get("spark_args", None)
        if task_specific_spark_args is not None and len(task_specific_spark_args) > 0:
            spark_args.update(task["spark_args"])
        return spark_args

    def create_command(self, task):
        location = task.get("script_location", "")
        command = task.get("command", "")
        kwargs = task.get("op_kwargs", {})
        bash_command = "cd {} && {} ".format(os.path.join(self.script_location, location), command)
        for key, value in kwargs.items():
            bash_command += " --{} {}".format(key, value)
        return bash_command

    def run_subprocess(self, task_id, bash_command, **kwargs):
        hook = SubprocessHook()
        try:
            command = "sudo -i -u {} bash -c '{}'".format(self.run_as_user, bash_command)
            result = hook.run_command(command=shlex.split(command))

            if kwargs.get("branch_on_failure"):
                if result.exit_code == 0:
                    next_task_id = kwargs.get("branch_on_success")
                else:
                    next_task_id = kwargs.get("branch_on_failure")
                print(f"Task {task_id} finished with exit code {result.exit_code}. Moving to ", next_task_id)
                return next_task_id

            elif result.exit_code != 0:
                raise Exception(f"Task {task_id} failed")

        except Exception as e:
            raise Exception(f"Task {task_id} failed: {e}")

        finally:
            try:
                hook.send_sigterm()
            except:
                print("SIGTERM failed, looks like process is already dead.")

    def init_config(self, task):
        if "op_kwargs" not in task:
            return

        if self.skip_if_exists_override and not task.get("is_extraction_job", False):
            task["op_kwargs"]["skip-if-exists"] = self.skip_if_exists_value

        for key in task["op_kwargs"]:
            if task["op_kwargs"][key] == "{base_config}":
                task["op_kwargs"][key] = self.base_config_path
            elif key == "source" and self.source_override:
                task["op_kwargs"][key] = self.source_override

    def notify_failure(self, context):
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        message = f"Job failed at step: {task_id}"
        self.page_user(dag_id, message, False)

    def notify_success(self, context):
        dag_id = context['dag'].dag_id
        message = "Job completed successfully."
        self.page_user(dag_id, message, True)

    def page_user(self, title, message, success):
        print("Callback: ", title, message)
        if not self.pager_key or not self.enable_alert:
            return

        data = {
            "routing_key": self.pager_key,
            "event_action": "trigger" if not success else "resolve",
            "payload": {
                "summary": f"{title}: {message}",
                "source": self.env,
                "severity": "error" if not success else "info"
            }
        }
        byte_length = str(sys.getsizeof(data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        response = requests.post(f"{self.pager_url}/enqueue", data=json.dumps(data), headers=headers, verify=False)
        print(response.status_code, response.text)


Dagger().run()