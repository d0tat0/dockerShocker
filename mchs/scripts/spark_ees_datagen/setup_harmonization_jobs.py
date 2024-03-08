import argparse
import json
import os
from datetime import datetime
from core.data_utils import UtilBase

FILE_NAME = "orchestrator.json"


REQUIREMENT_REPLACE = {
    "pandas": "pandas==1.3.5",
    "tqdm": "tqdm>=4.62.3",
    "protobuf": "protobuf"
}

IGNORE_REQUIREMENTS = {
    "pyspark", "delta-spark", "dotmap"
}


class Setup(UtilBase):
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.set_options(self.parser)
        self.options = self.parser.parse_args()

    def set_options(self, parser):
        parser.add_argument('--harmonization_jobs_conf', required=True)
        parser.add_argument('--jobs', required=False, default=None)

    def get_old_tag(self, file_path):
        if self.check_file_exists(file_path):
            d = self.get_json_data(file_path)
            return d['tag']
        return None

    def replace_github_access_token_in_file(self, file_path, token):
        fin = self.open_file(file_path)
        data = fin.read()
        data = data.replace("{{GITHUB_ACCESS_TOKEN}}", token)
        fin.close()
        self.write_lines(file_path, [data])

    def read_access_token_from_file(self, filepath):
        # one line of file containing the git access token
        with self.open_file(filepath) as fp:
            d = fp.read()
            d = d.strip()
            return d

    def setup_jobs(self, jobs):
        jobs_config_path = self.options.harmonization_jobs_conf
        jobs_config = self.get_json_data(jobs_config_path)

        # A file with only one line of access token automates manual password giving
        acces_token_filepath = os.path.join(jobs_config['resource_base_location'], jobs_config['access_token_file'])
        git_token = None

        try:
            git_token = self.read_access_token_from_file(acces_token_filepath)
        except:
            print(f"Unable to read access token from file. Enter token manually when required :( ")

        if git_token is not None:
            self.replace_github_access_token_in_file(jobs_config_path, git_token)

        jobs_config = self.get_json_data(jobs_config_path)

        resource_location = jobs_config["resource_base_location"]
        # repo would be cloned in clone_location_base/job_name/tag
        clone_location_base = os.path.join(resource_location, jobs_config["clone_location"])

        # venv would be created in  venv_location_base/job_name
        venv_location_base = os.path.join(resource_location, jobs_config["venv_location"])

        # data would be downloaded in data_location_base/job_name
        data_location_base = os.path.join(jobs_config["resource_cloud_location"], jobs_config['data_location'])

        # base path where the files would be downloaded
        job = jobs_config['jobs']

        # Handling error in Duke system where we get error " source command not found"
        is_ubuntu_os = jobs_config.get("is_ubuntu", False)
        if is_ubuntu_os:
            source_command = "."
        else:
            source_command = "source"

        failure_list = []
        for job_name in jobs_config["jobs"]:
            if jobs and job_name.lower() not in jobs:
                continue
            if not job[job_name]['enabled']:
                print(f"Job {job_name} is disabled.. skipping")
                continue
            print("------------------------------------------------\n\n")

            print("Setting up: {}\n".format(job_name))
            clone_location_wo_tag = os.path.join(clone_location_base, job_name)
            git_path = job[job_name]["git_link"]
            git_folder_name = git_path.split('/')[-1][:-4]
            current_tag = job[job_name]["tag"]
            # Using python3 as the default
            python_version = job[job_name].get("python_version", "python3.7")

            # folder where repo is cloned
            repo_path = os.path.join(clone_location_wo_tag, current_tag)
            old_tag = self.get_old_tag(os.path.join(repo_path, FILE_NAME))
            print(f"Old tag: {old_tag} New tag: {current_tag}")

            # don't do any of this if same tag exists
            if old_tag != current_tag:
                try:
                    print(f'\n\n Cloning repo.................')
                    self.run_cmd("mkdir -p {}".format(clone_location_wo_tag), True)
                    clone_location_with_tag = os.path.join(clone_location_wo_tag, current_tag)  # this is where the code resides
                    clone_cmd = "cd {}; git clone -b {} --single-branch {}".format(clone_location_wo_tag, current_tag, git_path)
                    self.run_cmd(clone_cmd, True)

                    # rename the dir from cloned name to current_tag name
                    mv_cmd = f'cd {clone_location_wo_tag};mv {git_folder_name} {current_tag}'
                    self.run_cmd(mv_cmd, True)
                    if job[job_name].get('has_submodule', False):
                        submodule = f'cd {clone_location_wo_tag}/{current_tag}; git submodule init; git submodule update'
                        self.run_cmd(submodule, True)

                    zip_path = os.path.join(clone_location_wo_tag, current_tag, f"{current_tag}.zip")
                    if job[job_name].get('relative_pythonpath'):
                        current_path = os.path.join(clone_location_wo_tag, current_tag, job[job_name]['relative_pythonpath'])
                        zip_cmd = f'cd {current_path}; zip -r {zip_path} * -x "*.zip"'
                    else:
                        zip_cmd = f'cd {clone_location_wo_tag}/{current_tag}; zip -r {zip_path} * -x "*.zip"'
                    self.run_cmd(zip_cmd, True)

                    # create virtualenv
                    create_virtualenv = True
                    if create_virtualenv:
                        print(f'\n\n Creating Virtualenv.................')
                        virtual_env_path = os.path.join(venv_location_base, job_name)
                        try:
                            cmd = "rm -rf {}".format(virtual_env_path)
                            print(f"Removing prev virtualenv: {cmd}")
                            self.run_cmd(cmd, True)
                        except:
                            print('Failed removal.. maybe the path does not exist')

                        if os.path.exists(virtual_env_path):
                            bkup = os.path.join(clone_location_base, job_name + "_bkup")
                            print(f"Taking a backup of old virtual_env {virtual_env_path} in {bkup}\n")
                            print(f"mv {virtual_env_path} {bkup}")
                            self.run_cmd(f"mv {virtual_env_path} {bkup}", True)

                        if not os.path.exists(venv_location_base):
                            self.run_cmd(f'mkdir -p {venv_location_base}', True)

                        virtual_env_cmd = f"cd {venv_location_base}; {python_version} -m venv --copies {job_name}"
                        self.run_cmd(virtual_env_cmd, True)

                        requirements_file = os.path.join(clone_location_with_tag,
                                                         job[job_name]["requirements_file"])

                        if git_token is not None:
                            print(f"Replacing Github access token in {requirements_file}")
                            self.replace_github_access_token_in_file(requirements_file, git_token)
                            print(f"Done replacing Github access token in {requirements_file}")

                        command = '''{} {}/bin/activate; pip install --upgrade setuptools'''.format(
                            source_command, virtual_env_path)
                        self.run_cmd(command, True)

                        command = '''{} {}/bin/activate; pip install --upgrade pip'''.format(
                            source_command, virtual_env_path)
                        self.run_cmd(command, True)

                        run_nltk = False
                        updated_requirements = []
                        with open(requirements_file, 'r') as file:
                            for line in file:
                                if 'nltk' in line:
                                    run_nltk = True
                                if line.strip().split("==")[0] in IGNORE_REQUIREMENTS:
                                    continue
                                updated_requirements.append(
                                    REQUIREMENT_REPLACE.get(line.strip().split("==")[0], line.strip()))
                        with open(requirements_file, 'w') as file:
                            for line in updated_requirements:
                                file.write(f"{line}\n")
                        print("UPDATED REQUIREMENTS")
                        print(updated_requirements)

                        pip_command = '''{} {}/bin/activate; pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host repo.nferx.com -r {}'''.format(
                            source_command,
                            virtual_env_path,
                            requirements_file)
                        print("Executing {}\n".format(pip_command))
                        self.run_cmd(pip_command, True)

                        if run_nltk:
                            nltk_command = f'source {virtual_env_path}/bin/activate; python -m nltk.downloader -d {virtual_env_path}/nltk_data stopwords'
                            self.run_cmd(nltk_command, True)
                        zip_venv = f'cd {venv_location_base}; zip -r {job_name}.zip {job_name}'
                        self.run_cmd(zip_venv, True)
                    else:
                        print(f'\t\t Not creating virtualenv for {job_name}')

                    print("\n\nCreating Resources Folder...\n")
                    data_location = os.path.join(data_location_base, job_name)
                    if not self.check_file_exists(data_location) and not data_location.startswith('gs://'):
                        cmd = f'mkdir -p {data_location}'
                        self.run_cmd(cmd, True)

                    cloud_path = job[job_name].get("resources_cloud_path")
                    if cloud_path and cloud_path.startswith('gs://'):
                        print("\n\nCopying Resources...\n")
                        print(f"Copying {cloud_path} into {data_location} \n")
                        cmd = f"gsutil -m cp -r {cloud_path} {data_location}"
                        self.run_cmd(cmd, True)
                    else:
                        print(f'\t\t Not downloading gcloud resources for {job_name}')

                    # write orchestrator.json indicating end of operations
                    with open(os.path.join(repo_path, FILE_NAME), 'w') as fp:
                        print(f'Generating info file for current repo info: {os.path.join(repo_path, FILE_NAME)}')
                        out_d = {'tag': current_tag, 'name': job_name}
                        now = datetime.now()
                        out_d['timestamp'] = now.strftime("%d/%m/%Y %H:%M:%S")

                        json.dump(out_d, fp)

                    # symlink the repo folder to latest
                    self.run_cmd(f'cd {os.path.join(clone_location_base, job_name)}; ln -s {current_tag} latest', True)
                    print("-----------------------------------\n\n")
                except Exception as e:
                    print(e)
                    print(f'***** {job_name} {current_tag} setup failed.. clearing the corresponding dirs ****** ')
                    self.rmtree(os.path.join(clone_location_base, job_name, current_tag))
                    self.rmtree(os.path.join(venv_location_base, job_name))
                    self.rmtree(os.path.join(data_location_base, job_name))
                    failure_list.append(job_name + '_' + current_tag)
            else:
                print(f"{job_name} with tag: {current_tag} already exists. Not downloading")

        print(f"\n\n**** All jobs that failed {failure_list}")
        if len(failure_list) > 0:
            raise Exception

    def run(self):
        jobs = self.options.jobs
        jobs = jobs.split(",") if jobs else []
        if self.options.harmonization_jobs_conf == "../../config/harmonisation_config.json":
            self.setup_jobs(jobs)


if __name__ == "__main__":
    obj = Setup()
    obj.run()