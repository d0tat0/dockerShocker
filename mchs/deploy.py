import os
import json
import glob
import argparse


def deploy_file(src_path, dest_path, user):
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
        create_dir_cmd = f"sudo -u {user} mkdir -p {dest_dir}"
        execute_command(create_dir_cmd)
    copy_cmd = f"sudo -u {user} cp {src_path} {dest_path}"
    execute_command(copy_cmd)


def deploy_dir(src_dir, dest_dir, user):
    clear_dir_cmd = f"sudo -u {user} rm -rf {dest_dir}"
    execute_command(clear_dir_cmd)
    create_dir_cmd = f"sudo -u {user} mkdir -p {dest_dir}"
    execute_command(create_dir_cmd)
    copy_cmd = f"sudo -u {user} cp -r {src_dir}/* {dest_dir}"
    execute_command(copy_cmd)


def deploy_json(json_data, dest_path, user):
    tmp_config_path = f"/tmp/{os.path.basename(dest_path)}_config.json"
    with open(tmp_config_path, "w") as f:
        json.dump(json_data, f, indent=4)
    deploy_file(tmp_config_path, dest_path, user)
    os.remove(tmp_config_path)


def execute_command(cmd):
    status = os.system(cmd)
    if status != 0:
        raise Exception(f"Command {cmd} failed with status {status}")
    print(f"Executed command: {cmd}")

def deploy():

    parser = argparse.ArgumentParser(description="Orchestrator Environment Manager")
    parser.add_argument('-A', '--config-alias', type=str, help='Config alias to deploy defined in deploy_config.json')
    parser.add_argument('-M', '--user-master', type=str, default='False', help='Use only spark-master for execution')
    parser.add_argument('-VR', '--venv-refresh', type=bool, default=False, help='Zip/Re-zip venv')
    args = parser.parse_args()
    deploy_config = json.load(open("deploy_config.json"))
    deploy_config = deploy_config[args.config_alias]
    config_alias = args.config_alias.lower()

    amc = deploy_config["amc"]
    env_config = json.load(open(os.path.join("configs", "env", amc, deploy_config["env_config"])))
    run_as_user = env_config["run_as_user"]
    root_deploy_location = env_config["deploy_location"]
    local_tmp_dir = env_config.get("local_tmp_dir")
    python = env_config["python"]
    venv_location = env_config["venv_location"]
    airflow_home = env_config["airflow_home"]
    airflow_user = env_config.get("airflow_user")

    # CLEAN UP DEPLOY LOCATION
    deploy_location = os.path.join(root_deploy_location, config_alias)
    clear_cmd = f"sudo -u {run_as_user} rm -rf {deploy_location}"
    execute_command(clear_cmd)


    # DEPLOY DAGS
    config_file_path = os.path.join("configs", "dags", amc, deploy_config["dags_config"])
    deploy_config_path = os.path.join(airflow_home, "dags", config_alias, "dags_config.json")
    deploy_file(config_file_path, deploy_config_path, "root")
    for dag_script in glob.glob(os.path.join("configs", "dags", "*.py")):
        deploy_script_path = os.path.join(airflow_home, "dags", config_alias, os.path.basename(dag_script))
        deploy_file(dag_script, deploy_script_path, "root")


    # DEPLOY ENVIRONMENT CONFIG
    if args.user_master.lower() == "true":
        env_config["spark_args"]["master"] = "local[*]"
        print("Using local spark master")
    deploy_config_path = os.path.join(airflow_home, "dags", config_alias, "env_config.json")
    deploy_json(env_config, deploy_config_path, "root")


    # DEPLOY ORCHESTRATOR DATA + SCHEMA CONFIG
    data_config_path = os.path.join("configs", "data", amc, deploy_config["data_config"])
    config_dict = json.load(open(data_config_path))
    schema_config_path = os.path.join("configs", "schema", amc, deploy_config["schema_config"])
    config_dict.update(json.load(open(schema_config_path)))

    # Replace mount points in config
    for orig_disc, mount_point in env_config.get("mount_redirects", {}).items():
        config_dict["source_dir"]["root_dir"] = config_dict["source_dir"]["root_dir"].replace(orig_disc, mount_point)
        config_dict["datagen_dir"]["root_dir"] = config_dict["datagen_dir"]["root_dir"].replace(orig_disc, mount_point)

    # Replace local tmp dir in config
    if local_tmp_dir:
        config_dict["datagen_dir"]["local_tmp_dir"] = local_tmp_dir

    deploy_config_path = os.path.join(deploy_location, "config", "config.json")
    deploy_json(config_dict, deploy_config_path, run_as_user)


    # SETUP VIRTUAL ENVIRONMENT
    if not os.path.exists(venv_location):
        requirements_file = "requirements.txt"
        deploy_file_path = os.path.join(root_deploy_location, requirements_file)
        deploy_file(requirements_file, deploy_file_path, run_as_user)

        venv_create_cmd = f"sudo {python} -m venv {venv_location}"
        execute_command(venv_create_cmd)
        activate_cmd = f"source {venv_location}/bin/activate"
        execute_command(activate_cmd)
        trusted_hosts = "--trusted-host repo.nferx.com"
        cython_install_cmd = f"sudo {venv_location}/bin/pip install Cython {trusted_hosts}"
        execute_command(cython_install_cmd)
        install_reqs_cmd = f"sudo {venv_location}/bin/pip install -r requirements.txt {trusted_hosts}"
        execute_command(install_reqs_cmd)
        venv_folder = venv_location.split('/')[-1]
        root_venv = os.path.dirname(venv_location)
        zip_venv_cmd = f"cd {root_venv}; sudo -u {run_as_user} zip -r {venv_folder}.zip {venv_folder}/*"
        print(f"Executing {zip_venv_cmd}")
        execute_command(zip_venv_cmd)


    # CREATE FRESH ZIP FOR CODE/VENV
    pyenv_folder = venv_location.split("/")[-1]
    if args.venv_refresh:  # or not os.path.exists(pyenv_folder):
        zip_venv = f"cd {venv_location}; cd ..; sudo -u {run_as_user} zip -r {pyenv_folder}.zip {pyenv_folder}"
        execute_command(zip_venv)

    # submodule cloning
    #submodule_init_command = "git submodule init "
    #execute_command(submodule_init_command)
    #update_command = "git submodule update --recursive --remote"
    #execute_command(update_command)


    # DEPLOY SCRIPTS
    deploy_dir("scripts", os.path.join(deploy_location, "scripts"), run_as_user)
    deploy_dir("jars", os.path.join(root_deploy_location, "jars"), run_as_user)
    scripts_location = os.path.join(deploy_location, "scripts")
    zip_scripts_cmd = f"cd {scripts_location}; sudo -u {run_as_user} zip -r scripts.zip *"

    # TO DO -- DEPLOY HARMONISATION
    config_file_path = os.path.join("configs", "harmonisation", amc, deploy_config["harmonisation_config"])
    deploy_config_path = os.path.join(deploy_location, "config", "harmonisation_config.json")
    deploy_file(config_file_path, deploy_config_path, run_as_user)
 
    # Configure OMOP configs
    if "omop_config_folder" in deploy_config:
        omop_config_folder = deploy_config["omop_config_folder"]
        omop_config_location_source = os.path.join(deploy_location, "scripts", "omop", "config", omop_config_folder)
        omop_config_location = os.path.join(deploy_location, "scripts", "omop", "config")
        omop_config_cp_cmd = f"sudo -u {run_as_user} cp {omop_config_location_source}/* {omop_config_location}"
        execute_command(omop_config_cp_cmd)
        # omop_credentials_copy = f"sudo -u {run_as_user} gsutil cp gs://cdap-orchestrator-offline-releases/OMOP_METADATA/creds.json {omop_config_location}"
        # execute_command(omop_credentials_copy)

    # Deploy cluster mode
    scripts_location = os.path.join(deploy_location, "scripts")
    zip_scripts_cmd = f"cd {scripts_location}; sudo -u {run_as_user} zip -r scripts.zip *"
    print(f"Executing {zip_scripts_cmd}")
    execute_command(zip_scripts_cmd)

    if deploy_config.get("source_concept_meta_config"):
        config_file_path = os.path.join("configs", "harmonisation", amc, deploy_config["source_concept_meta_config"])
        deploy_config_path = os.path.join(deploy_location, "config", "source_concept_meta.json")
        deploy_file(config_file_path, deploy_config_path, run_as_user)

    if deploy_config.get("validation_config"):
        config_file_path = os.path.join("configs", "validations", amc, deploy_config["validation_config"])
        deploy_config_path = os.path.join(deploy_location, "config", "GROUPBYColumnQueryTable.json")
        deploy_file(config_file_path, deploy_config_path, run_as_user)

    if deploy_config.get("post_processor_config"):
        config_file_path = os.path.join("configs", "post_processor", amc, deploy_config["post_processor_config"])
        deploy_config_path = os.path.join(deploy_location, "config", "post_processor_config.json")
        deploy_file(config_file_path, deploy_config_path, run_as_user)

    # LISTING DAGS
    refresh_dag_cmd = "sudo touch " + os.path.join(airflow_home, "dags", config_alias, "*.py")
    execute_command(refresh_dag_cmd)

    if airflow_user == "root":
        list_dag_cmd = f"sudo airflow dags list-import-errors"
    elif airflow_user:
        list_dag_cmd = f"sudo -u {airflow_user} airflow dags list-import-errors"
    else:
        list_dag_cmd = "airflow dags list-import-errors"
    #DEPRECATED in latest airflow
    #execute_command(list_dag_cmd)


if __name__ == "__main__":
    deploy()





