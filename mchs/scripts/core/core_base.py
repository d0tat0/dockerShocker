import glob
import json
import shutil
import subprocess
from datetime import datetime
from typing import Dict

from core.config_vars import *

HARMONISATION_CONFIG_FILENAME = "harmonisation_config.json"
POST_PROCESSOR_CONFIG_FILENAME = "post_processor_config.json"

class CoreBase(object):

    def __init__(self, config_file) -> None:
        self.config_file = config_file if config_file else os.path.join(CORE_CONFIG_DIR, "config.json")
        if not os.path.exists(self.config_file):
            # Cluster mode support
            self.config_file = "config.json"
        self.harmonisation_config_file = os.path.join(os.path.dirname(self.config_file), HARMONISATION_CONFIG_FILENAME)
        self.post_processor_config_file = os.path.join(os.path.dirname(self.config_file), POST_PROCESSOR_CONFIG_FILENAME)
        self.SYSTEM_DICT: Dict = None

    def get_dirs(self):
        print(f"Loading SYSTEM_DICT from {self.config_file}")
        self.SYSTEM_DICT = self.get_json_data(self.config_file)

        amc_config = self.SYSTEM_DICT['amc_config']
        self.DATA_ENV = amc_config['name']
        self.DIM_PATIENT = amc_config['dim_patient']
        self.PATIENT_DK = amc_config['patient_dk']
        # For OB_DELIVERY and similar tables
        self.ADDITIONAL_PDK = amc_config.get('additional_pdk_fields', [])
        self.PATIENT_CLINIC_NBR = amc_config['clinic_nbr']
        self.PATIENT_MERGED_FLAG = amc_config.get('patient_merged_flag')
        self.RELATIVE_DTM = amc_config['relative_dtm']
        self.ENCOUNTER_EPOCH = amc_config['encounter_epoch']
        self.BIRTH_DATE = amc_config['birth_date']
        self.DTM_FORMAT = amc_config.get('dtm_format')
        self.BIRTH_DATE_FORMAT = amc_config['birth_date_format']
        self.BIG_TABLES = amc_config['big_tables']
        self.SIGNAL_TABLES = amc_config['signal_tables']
        self.HYC_TABLES = amc_config['hyc_tables']
        self.csv_separator = amc_config.get("csv_separator", PIPE_SEP)
        self.IS_CSV_MODE = amc_config.get("is_csv_mode", True)

        orch_dir_dict = self.SYSTEM_DICT['datagen_dir']
        self.OLD_MAPS_DIR = orch_dir_dict.get('old_maps_dir')
        self.RESOURCE_OLD_MAPS_DIR = orch_dir_dict.get('old_maps_dir')
        self.RELEASE_BUCKET = orch_dir_dict.get('release_bucket')
        self.ROOT_DIR = orch_dir_dict['root_dir']
        self.ROOT_SOURCE_DIR = self.SYSTEM_DICT['source_dir']['root_dir']
        self.SOURCE_DATA_VERSION = orch_dir_dict.get('source_data_version', 1)
        self.LATEST_DELTA_DIR = orch_dir_dict.get('latest_delta_tables_dir', LATEST_DELTA_DIR)
        using_remote_dir = not self.ROOT_DIR.startswith('/')

        self.LOCAL_ROOT_DIR = orch_dir_dict.get('local_root_dir')
        if not self.LOCAL_ROOT_DIR and using_remote_dir:
            self.LOCAL_ROOT_DIR = self.ROOT_DIR

        self.__init_sample_run()
        self.WRITE_ROOT_DIR = orch_dir_dict.get("write_root_dir", self.ROOT_DIR)
        self.__sample_run_overrides()

        self.RUN_STATE_DIR = os.path.join(self.ROOT_DIR, 'RUN_STATE')
        self.LOCAL_TMP_DIR = orch_dir_dict.get('local_tmp_dir')
        if self.SAMPLE_RUN:
            os.makedirs(self.LOCAL_ROOT_DIR, exist_ok=True)
        if not self.LOCAL_TMP_DIR and self.LOCAL_ROOT_DIR and os.path.exists(self.LOCAL_ROOT_DIR):
            self.LOCAL_TMP_DIR = os.path.join(self.LOCAL_ROOT_DIR, 'TMP')
            os.makedirs(self.LOCAL_TMP_DIR, exist_ok=True)

        if using_remote_dir and (not self.LOCAL_TMP_DIR or not os.path.exists(self.LOCAL_TMP_DIR)):
            raise Exception("LOCAL_ROOT_DIR/LOCAL_TMP_DIR must be set and available while using remote root dir")

        self.RUN_STATE_LOG = self.load_run_version_state()
        self.VERSION_LIST = sorted(list(self.RUN_STATE_LOG.keys()), reverse=True)
        self.RUN_VERSION = self.VERSION_LIST[0] if self.VERSION_LIST else None
        self.NFERX_VERSION = f"{self.SOURCE_DATA_VERSION}.000"

        self.RUN_MODE = self.SYSTEM_DICT['run_mode']
        self.RAW_SRC_DIR_DICT = self.SYSTEM_DICT['source_dir']['raw_src_dirs']
        self.RAW_SOURCE_DIRS = None

        # EXTRACTIONS/HARMONIZATIONS
        self.extraction_configs = self.SYSTEM_DICT.get("extraction_jobs", {})
        self.harmonized_root_dir = None
        self.harmonized_out_dir = None
        self.harmonized_meta_dir = None
        self.dim_syn_interim_dir_name = "DIM_SYN_INTERIM"
        if self.extraction_configs:
            self.harmonized_root_dir = self.extraction_configs["output_root_dir"]
            self.harmonized_out_dir = self.extraction_configs["final_output_dir"]
            self.harmonized_meta_dir = self.extraction_configs["output_meta_dir"]

        if self.RUN_VERSION:
            self.INPUT_SOURCE_DIR = os.path.join(self.ROOT_SOURCE_DIR, self.RUN_VERSION)
            self.DATA_DIR = os.path.join(self.WRITE_ROOT_DIR, self.RUN_VERSION)
            self.ROOT_DATA_DIR = os.path.join(self.ROOT_DIR, self.RUN_VERSION)
            self.DATAGEN_DIR = os.path.join(self.DATA_DIR, "DATAGEN")
            self.DIM_SYN_INTERIM_DIR = os.path.join(self.DATA_DIR, self.harmonized_root_dir, self.dim_syn_interim_dir_name, self.harmonized_out_dir)
            self.NFERX_DATA_DIR = os.path.join(self.WRITE_ROOT_DIR, self.NFERX_VERSION)

            self.LOCAL_DATA_DIR = None
            if self.LOCAL_ROOT_DIR:
                self.LOCAL_DATA_DIR = os.path.join(self.LOCAL_ROOT_DIR, self.RUN_VERSION)
            elif self.LOCAL_TMP_DIR:
                self.LOCAL_DATA_DIR = os.path.join(self.LOCAL_TMP_DIR, self.RUN_VERSION)
            else:
                print("WARNING: LOCAL_ROOT_DIR or LOCAL_TMP_DIR must be set while using remote root dir")

            self.RAW_SOURCE_DIRS = self.SYSTEM_DICT['source_dir']['raw_src_dirs'][self.RUN_VERSION]["bucket"]

        if isinstance(self.RAW_SOURCE_DIRS, str):
            self.RAW_SOURCE_DIRS = [self.RAW_SOURCE_DIRS]

        data_size_dict = self.SYSTEM_DICT['data_sizes']
        self.MAX_BUCKETS = data_size_dict['max_buckets']
        self.MAX_SUB_BUCKETS = data_size_dict['max_sub_buckets']
        self.MAX_PATIENTS = data_size_dict['max_patients']
        self.MAX_PARTITIONS = data_size_dict['max_partitions']
        self.MAX_BLOCKS = data_size_dict['max_blocks']
        self.MAX_THRESHOLD = data_size_dict['max_threshold']
        self.PATIENT_PARTITION_SIZE = self.MAX_PATIENTS // self.MAX_PARTITIONS
        self.PROCESS_PARTITION_SIZE = self.MAX_PATIENTS // MAX_PROCESS

    def __init_sample_run(self):
        sample_run_data = self.SYSTEM_DICT.get('sample_run', {})
        self.SAMPLE_RUN = sample_run_data.get("enabled", False)
        if not self.SAMPLE_RUN:
            return

        self.SAMPLE_GT = sample_run_data.get("ground_truth", None)
        self.SAMPLE_PID_SOURCE = os.path.join(self.ROOT_DIR, sample_run_data.get("sample_pid_folder", ""))
        sample_user = sample_run_data.get("user")
        self.SAMPLE_USER = os.path.join(sample_user,
                                        datetime.now().strftime("%Y%m%d_%H-%M")) if sample_user else self.SAMPLE_GT
        self.SAMPLE_MAX_SIZE = sample_run_data.get("max_sample_size", 0)
        self.SAMPLE_PID_GT = sample_run_data.get("sample_pid_gt", None)
        if self.SAMPLE_PID_GT:
            self.SAMPLE_PID_SOURCE = os.path.join(self.ROOT_DIR, self.SAMPLE_PID_GT,
                                                  sample_run_data.get("sample_pid_folder", ""))
        self.SAMPLE_SIZE = sample_run_data.get("sample_size", 0)
        self.SAMPLE_COL = sample_run_data.get("sample_pid_col", 0)
        self.SAMPLE_MANUAL_ENTRIES = sample_run_data.get("manual_entries", [])
        self.SAMPLE_USE_WRITE_ROOT_DIR = sample_run_data.get("use_user_root_dir", False)

    def __sample_run_overrides(self):
        if self.SAMPLE_RUN:
            self.OLD_MAPS_DIR = ""
            self.ROOT_SOURCE_DIR = os.path.join(self.ROOT_SOURCE_DIR, self.SAMPLE_GT)
            self.WRITE_ROOT_DIR = os.path.join(self.ROOT_DIR, self.SAMPLE_USER)
            self.ROOT_DIR = os.path.join(self.ROOT_DIR, self.SAMPLE_GT)
            self.LOCAL_ROOT_DIR = os.path.join(self.LOCAL_ROOT_DIR, self.SAMPLE_USER)
            if self.SAMPLE_USE_WRITE_ROOT_DIR:
                # Will fail in reading last dir/validation against old data
                # ToDo: Put checks on validation
                self.ROOT_DIR = self.WRITE_ROOT_DIR

    def set_source_datagen_info(self, obj):
        self.SYSTEM_DICT = obj.SYSTEM_DICT
        self.MAX_PATIENTS = obj.MAX_PATIENTS
        self.MAX_PARTITIONS = obj.MAX_PARTITIONS
        self.MAX_BLOCKS = obj.MAX_BLOCKS
        self.PATIENT_PARTITION_SIZE = self.MAX_PATIENTS // self.MAX_PARTITIONS
        self.PROCESS_PARTITION_SIZE = self.MAX_PATIENTS // MAX_PROCESS

    def load_run_version_state(self, load_history=False):
        run_state_log = {}
        run_version_log = os.path.join(self.RUN_STATE_DIR, "run_versions.txt")
        if self.glob(run_version_log):
            for line in self.open_file(run_version_log):
                if not line.strip():
                    continue
                run_version, run_mode, start_dtm, end_dtm, status, run_source = line.strip().split(PIPE_SEP)
                if run_mode == RunMode.Full.value and not load_history:
                    run_state_log = {}
                run_state_log.setdefault(run_version, {
                    "RUN_MODE": run_mode,
                    "START_DTM": start_dtm,
                    "END_DTM": end_dtm,
                    "STATUS": status,
                    "RUN_SOURCE": run_source
                })
        else:
            print(f"RUN version log not found at {run_version_log}")
        return run_state_log

    def run_cmd(self, cmd, log=False):
        if log:
            print(cmd)
        status = os.system(cmd)
        if status:
            raise Exception(f'ERROR: {status} {cmd}')

    def scan_data_dir(self, *dir_parts, run_version=None, base_version=None, log=True):
        print(f"**** Scanning for {dir_parts}")
        if run_version is None:
            run_version = self.RUN_VERSION
        if base_version is None:
            base_version = self.SOURCE_DATA_VERSION

        scanned_versions = []
        data_dir_path = None
        for version_str in self.VERSION_LIST:
            if round(float(version_str),3) < round(float(base_version),3) or\
                    round(float(version_str),3) > round(float(run_version),3):
                continue
            scanned_versions.append(version_str)

            if dir_parts[0] in ["DATAGEN", "FACT_MAPS_VERSIONED", "SYN_DATA", "WAVEFORM"]:
                parquet_dir_parts = [f"{dir_parts[0]}.parquet"] + list(dir_parts)[1:]
                data_dir_path = os.path.join(self.ROOT_DIR, version_str, *parquet_dir_parts)

            if not data_dir_path or not self.glob(data_dir_path):
                data_dir_path = os.path.join(self.ROOT_DIR, version_str, *dir_parts)

            if data_dir_path and self.glob(data_dir_path):
                if log:
                    print(f"Scanned for {os.path.join(*dir_parts)} in {scanned_versions}, found in {version_str}")
                return data_dir_path, float(version_str)

        if log:
            print(f"Scanned for {os.path.join(*dir_parts)} in {scanned_versions}")
            print(f"No data found.")
        return None, None

    def get_dir_size_in_gb(self, dir_path):
        total_size = 0
        if dir_path.startswith('gs://'):
            cmd = "gsutil du -s " + dir_path
            try:
                total_size = int(subprocess.check_output(cmd, shell=True).decode().split()[0])
            except:
                pass
        else:
            for dirpath, dirnames, filenames in os.walk(dir_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    total_size += os.path.getsize(fp)
        return total_size/1024/1024/1024

    def get_line_count(self, dir_path):
        total_lines = 0
        if dir_path.startswith('gs://'):
            for _path in self.glob(dir_path):
                cmd = f"gsutil cat {_path} | wc -l"
                try:
                    total_lines += int(subprocess.check_output(cmd, shell=True).decode().split()[0])
                except:
                    pass
        else:
            for dirpath, dirnames, filenames in os.walk(dir_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    cmd = f"wc -l {fp}"
                    try:
                        total_lines += int(subprocess.check_output(cmd, shell=True).decode().split()[0])
                    except:
                        pass
        return total_lines

    def glob(self, pattern:str):
        if pattern.startswith('gs://'):
            cmd = "gsutil -q ls " + pattern
            try:
                return subprocess.check_output(cmd, shell=True).decode().split()
            except:
                return []
        return glob.glob(pattern)

    def dir_glob(self, pattern:str):
        if pattern.startswith('gs://'):
            cmd = "gsutil -q ls " + pattern
            try:
                return subprocess.check_output(cmd, shell=True).decode().split()
            except:
                return []
        if not pattern.endswith('*'):
            pattern = os.path.join(pattern,'*')
        return glob.glob(pattern)

    def check_file_exists(self, file_path):
        if file_path.startswith('gs://'):
            cmd = f"gsutil ls {file_path}"
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode == 0:
                return True
            else:
                return False
        else:
            return os.path.exists(file_path)

    def open_file(self, file_name, encoding=None):
        if file_name.startswith('gs://'):
            temp_file_path = os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name))
            self.copy_file(file_name, temp_file_path)
            return open(temp_file_path, encoding=encoding)
        return open(file_name, encoding=encoding)

    def rename_file(self, orig_file_name, new_file_name):
        if orig_file_name.startswith('gs://'):
            cmd = "gsutil -q mv " + orig_file_name + " " + new_file_name
            subprocess.check_output(cmd, shell=True)
        else:
            os.makedirs(os.path.dirname(new_file_name), exist_ok=True)
            os.rename(orig_file_name, new_file_name)

    def copy_file(self, orig_file_name, new_file_name):
        if orig_file_name.startswith('gs://') and new_file_name.startswith('gs://'):
            cmd = f"gsutil -q cp {orig_file_name} {new_file_name}"
            subprocess.check_output(cmd, shell=True)
            print(f"File {orig_file_name} has been copied to {new_file_name}")
        elif orig_file_name.startswith('gs://'):
            cmd = f"gsutil -q cp {orig_file_name} -"
            try:
                output = subprocess.check_output(cmd, shell=True)
                with open(new_file_name, 'wb') as f:
                    f.write(output)
                print(f"File {orig_file_name} has been copied to {new_file_name}")
            except:
                print(f"Failed to copy {orig_file_name} to {new_file_name}")
        elif new_file_name.startswith('gs://'):
            cmd = f"gsutil -q cp - {new_file_name}"
            try:
                with open(orig_file_name, 'rb') as f:
                    input_data = f.read()
                subprocess.run(cmd, shell=True, input=input_data)
                print(f"File {orig_file_name} has been copied to {new_file_name}")
            except:
                print(f"Failed to copy {orig_file_name} to {new_file_name}")
        else:
            os.makedirs(os.path.dirname(new_file_name), exist_ok=True)
            shutil.copy(orig_file_name, new_file_name)
            print(f"File {orig_file_name} has been copied to {new_file_name}")

    def copy_directory(self, orig_dir, new_dir):
        if orig_dir.startswith('gs://') and new_dir.startswith('gs://'):
            cmd = f"gsutil -q -m cp -r {orig_dir} {new_dir}"
            subprocess.check_output(cmd, shell=True)
            print(f"Directory {orig_dir} has been copied to {new_dir}")
        elif orig_dir.startswith('gs://'):
            self.download_dir(orig_dir, new_dir)
        elif new_dir.startswith('gs://'):
            self.upload_dir(orig_dir, new_dir)
        else:
            shutil.copytree(orig_dir, new_dir)
            print(f"Directory {orig_dir} has been copied to {new_dir}")

    def upload_dir(self, local_dir, remote_dir):
        cmd = f"gsutil -q -m cp -r {local_dir}/* {remote_dir}"
        subprocess.check_output(cmd, shell=True)

    def backup_copy(self, orig_file_name, new_file_name):
        new_file_name  = new_file_name if new_file_name.endswith('/') else new_file_name + '/'
        print(f"Copying {orig_file_name} to {new_file_name}")
        cmd = f"gsutil -q -m cp -r {orig_file_name} {new_file_name}"
        subprocess.check_output(cmd, shell=True)
        print(f"File {orig_file_name} has been copied to {new_file_name}")

    def move_backup_table(self, backup_table_path):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"{backup_table_path}_{timestamp}"
        print(f' moving {backup_table_path} to {new_file_name}')
        if backup_table_path.startswith('gs://'):
            cmd = f"gsutil -q -m mv {backup_table_path} {new_file_name}"
            subprocess.check_output(cmd, shell=True)
        else:
            cmd = f"mv {backup_table_path} {new_file_name}"
            subprocess.check_output(cmd, shell=True)

    def download_dir(self, remote_dir, local_dir):
        cmd = f"gsutil -q cp -r {remote_dir}/* {local_dir}"
        subprocess.check_output(cmd, shell=True)

    def write_lines(self, file_name, lines = []):
        out_file = file_name
        if file_name.startswith('gs://'):
            out_file = os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name))
        else:
            os.makedirs(os.path.dirname(out_file), exist_ok=True)

        with open(out_file, 'w') as f:
            if len(lines) > 0:
                f.write("\n".join([l.strip() for l in lines]) + "\n")

        if file_name.startswith('gs://'):
            cmd = "gsutil -q cp " + os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)) + " " + file_name
            subprocess.check_output(cmd, shell=True)
            os.remove(os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)))

    def rmtree(self, file_path):
        if file_path.startswith('gs://'):
            cmd = "gsutil -q -m rm -r " + file_path
            subprocess.run(cmd, shell=True)
        else:
            cmd = "rm -rf " + file_path
            subprocess.check_output(cmd, shell=True)

    def get_json_data(self, file_name):
        if not self.glob(file_name):
            return {}
        return json.loads(self.open_file(file_name).read())

    def dump_json_data(self, file_name, obj):
        if file_name.startswith('gs://'):
            with open(os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)), 'w') as f:
                f.write(json.dumps(obj, indent=4))
            cmd = "gsutil -q cp " + os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)) + " " + file_name
            subprocess.check_output(cmd, shell=True)
            os.remove(os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)))
        else:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            open(file_name, 'w').write(json.dumps(obj, indent=4))

    def dump_string_data(self, file_name, obj):
        if file_name.startswith('gs://'):
            with open(os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)), 'w') as f:
                f.write(obj)
            cmd = "gsutil -q cp " + os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)) + " " + file_name
            subprocess.check_output(cmd, shell=True)
            os.remove(os.path.join(self.LOCAL_TMP_DIR, os.path.basename(file_name)))
        else:
            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            open(file_name, 'w').write(obj)

    def get_values(self, line, delimiter=PIPE_SEP):
        return line.strip().split(delimiter)

    def start_processes(self, create_func, process_count):
        jobs = []
        for i in range(process_count):
            worker = create_func(i)
            jobs.append(worker)
            worker.start()
        for j in jobs:
            j.join()
            if j.exitcode != 0:
                error_message = j.exception if hasattr(j, "exception") else None
                error_info = "Subprocess {} Failed. Exit code: {}".format(j.name, j.exitcode)
                if error_message:
                    error_info += "\nError Message: {}".format(error_message)
                print(error_info)
                #raise Exception('Subprocess {} Failed'.format(j.name))

    def run_main(self):
        pass

    def run(self):
        self.options.run = self.get_opt_list(self.options.run)
        self.options.source = self.options.source.upper()
        self.options.max_process = int(self.options.max_process)

        start_time = datetime.now()
        self.run_main()
        end_time = datetime.now()
        print('Time Taken: %s' % (end_time - start_time, ))
