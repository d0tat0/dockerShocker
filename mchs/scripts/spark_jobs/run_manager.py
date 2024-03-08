import os
import glob
import subprocess
from datetime import datetime
from string import Template
from copy import deepcopy

from core.config_vars import RunMode, RunStatus, PIPE_SEP, DEFAULT_COUNTER
from spark_jobs.spark_job import SparkJob


class RunManager(SparkJob):

    def __init__(self, config_file=None) -> None:
        super().__init__(config_file)
        self.run_state_log = {}
        self.data_files = []
        self.dim_files = []
        self.transfer_files = []
        self.sample_load = False

    def run(self):
        if self.options.run == 'create':
            self.create_version(self.options.version, self.options.source, self.options.dry_run, self.options.source)
        elif self.options.run == 'release':
            self.release_version()
        elif self.options.run == 'sync_run_state':
            self.sync_run_state(self.RUN_VERSION if self.options.version is None else self.options.version)
        elif self.options.run == 'calc_stats':
            self.calc_stats(self.RUN_VERSION if self.options.version is None else self.options.version)

    def calc_stats(self, version):
        stats = {}
        source_dirs = self.RAW_SRC_DIR_DICT[version]["bucket"]
        if isinstance(source_dirs, str):
            source_dirs = [source_dirs]
        for source_dir in source_dirs:
            tables = self.glob(source_dir)
            for table in tables:
                stats.setdefault(table, {"size": 0, "line_count": 0})
                stats[table]["size"] += self.get_dir_size_in_gb(table)
                #stats[table]["line_count"] += self.get_line_count(table)

        data = ["table_path|size|line_count"]
        for table, table_stats in stats.items():
            data.append(f"{table}|{table_stats['size']}|{table_stats['line_count']}")

        run_version_stats = os.path.join(self.RUN_STATE_DIR, "run_versions_stats.txt")
        self.write_lines(run_version_stats, data)

    def sync_run_state(self, version):
        print(f"Syncing version {version}")
        remote_run_state_dir = os.path.join(self.RUN_STATE_DIR, version)
        local_run_state_dir = os.path.join(self.LOCAL_ROOT_DIR, "RUN_STATE")
        self.download_dir(remote_run_state_dir, local_run_state_dir)

        remote_run_versions_file = os.path.join(self.RUN_STATE_DIR, "run_versions.txt")
        local_run_versions_file = os.path.join(self.LOCAL_ROOT_DIR, "RUN_STATE", "run_versions.txt")
        self.copy_file(remote_run_versions_file, local_run_versions_file)

        version_dir = os.path.join(self.LOCAL_ROOT_DIR, version)
        os.makedirs(version_dir, exist_ok=True)

    def create_version(self, version, trigger, sample, sources):
        """
        Create a new version, Folder structure, scans for raw files,
        creates appropriate files

        @params
        version: Which version to run
        trigger: string denoting what to run
        sample: true implies load sample data. dry run
        sources: if you want to consume only a few tables
        """

        if not sources:
            sources = []
        else:
            sources = sources.split(",")
        sources = sources or self.RAW_SRC_DIR_DICT[version].get("sources", [])
        print(f"Run Manager {self.RUN_MODE} mode")

        self.sample_load = sample and sample.lower() == "true"
        if self.sample_load:
            print("Sample Load Mode")
            trigger = "SAMPLE_DATA_LOAD"

        if not self.glob(self.RUN_STATE_DIR):
            os.makedirs(self.RUN_STATE_DIR, exist_ok=True)
            run_version_log = os.path.join(self.RUN_STATE_DIR, "run_versions.txt")
            self.write_lines(run_version_log)
            self.run_state_log = {}
        else:
            self.run_state_log = self.load_run_version_state()

        if ((version and float(version) * 1000 % 2 != 0) or self.RUN_MODE == RunMode.Full.value):
            self.scan_for_new_files(version, sources)
            if not any(self.data_files):
                raise Exception("No new data files found")
            if self.RUN_MODE == RunMode.Delta.value and not self.valid_incremental_files():
                print("Incr-Data files are empty, skipping data run!")
                return

        # self.setup_ees_resources()
        self.create_run_version(version, trigger)
        self.create_version_dirs()
        self.update_run_data_files()
        self.update_run_version_state()

    def update_run_version_state(self):
        run_state_log_history = self.load_run_version_state(load_history=True)
        run_state_log_history.update(self.run_state_log)
        run_version_log = os.path.join(self.RUN_STATE_DIR, "run_versions.txt")
        run_logs = []
        for version, meta in run_state_log_history.items():
            run_logs.append("{0}|{1}|{2}|{3}|{4}|{5}\n".format(version,
                meta["RUN_MODE"],
                meta["START_DTM"],
                meta["END_DTM"],
                meta["STATUS"],
                meta["RUN_SOURCE"]
            ))
        self.write_lines(run_version_log, run_logs)
        print("Updated run_version_log:", run_version_log)

    def load_file_patterns(self, file_list, file_path_patterns, counter):
        for file_path_pattern in file_path_patterns:
            file_paths = self.glob(file_path_pattern)
            count = 0
            for file_path in file_paths:
                file_list.append((file_path, counter))
                count += 1
                if self.sample_load:
                    break
            print("Found {} files matching pattern: {}".format(count, file_path_pattern))
        return file_list

    def get_file_patterns(self, source, file_type):
        if file_type == "data":
            file_patterns = self.SYSTEM_DICT["amc_config"]["data_file_pattern"]
        else:
            file_patterns = self.SYSTEM_DICT["amc_config"]["info_file_pattern"]

        file_name_case = self.SYSTEM_DICT["amc_config"]["file_name_case"]
        if file_name_case == "upper":
            source = source.upper()
        elif file_name_case == "lower":
            source = source.lower()
        else:
            source = source
        if isinstance(file_patterns, str):
            file_patterns = [file_patterns]
        file_patterns = [Template(i).substitute(source=source) for i in file_patterns]
        return file_patterns

    def scan_for_new_files(self, version, sources):
        counter = deepcopy(DEFAULT_COUNTER)
        source_dirs = self.RAW_SRC_DIR_DICT[version]["bucket"]
        if isinstance(source_dirs, str):
            source_dirs = [source_dirs]
        for source_dir in source_dirs:
            print(f"Scanning {source_dir}")
            self.scan_for_dim_files(source_dir, counter, sources)
            for data_table in self.SYSTEM_DICT["data_tables"].values():
                table_name = data_table["table_name"]
                if sources and table_name not in sources:
                    continue
                data_file_patterns = self.get_file_patterns(table_name, "data")
                self.data_files = self.load_file_patterns(self.data_files, [os.path.join(source_dir, i) for i in data_file_patterns], counter)
            print("Total {} data files".format(len(self.data_files)))
            counter += 1

    def scan_for_dim_files(self, source_dir, counter, sources):
        for dim_config in self.SYSTEM_DICT["info_tables"].values():
            dim_dir_name = dim_config["incr_dir_name"]
            if sources and dim_dir_name not in sources:
                continue
            dim_file_patterns = self.get_file_patterns(dim_dir_name, "info")
            self.dim_files = self.load_file_patterns(self.dim_files, [os.path.join(source_dir, i) for i in dim_file_patterns], counter)
        print("Total {} dim files".format(len(self.dim_files)))

    def scan_for_transfer_files(self, source_dir):
        all_transfer_files = set(self.glob(os.path.join(source_dir, "*transfer*.done")))
        if not any(all_transfer_files):
            all_transfer_files = self.curate_transfer_file_for_source_dir(source_dir)
        processed_transfer_files = self.load_processed_transfer_files()
        unprocessed_transfer_files = all_transfer_files.difference(processed_transfer_files)
        print("Found {} UnProcessed Transfer-Files".format(len(unprocessed_transfer_files)))
        return unprocessed_transfer_files

    def curate_transfer_file_for_source_dir(self, source_dir):
        transfer_file_name = os.path.join("/tmp", source_dir[1:], "curated_transfer.done")
        with open(transfer_file_name, 'w') as f:
            for file_path in self.glob(os.path.join(source_dir, "*.*")):
                f.write(file_path + "\n")
        return set([transfer_file_name])

    def load_processed_transfer_files(self):
        processed_transfer_files = set()
        for version, meta in self.run_state_log.items():
            if meta["STATUS"] != RunStatus.Success.value:
                continue
            file_path = os.path.join(self.RUN_STATE_DIR, version, "transfer_files.txt")
            for line in self.open_file(file_path):
                processed_transfer_files.add(line.strip())
                print(line.strip(), "processed by", version)
        return processed_transfer_files

    def process_transfer_file(self, source_dir, done_file):
        if done_file in self.transfer_files:
            return
        self.transfer_files.append(done_file)
        count = 0
        for i, line in enumerate(self.open_file(done_file)):
            if i == 0:
                continue
            data_file_name = line.strip().split('|')[0]
            data_file_path = os.path.join(source_dir, data_file_name)
            self.data_files.append(data_file_path)
            count += 1
        print("{0} => {1}".format(os.path.basename(done_file), count))

    def valid_incremental_files(self):
        dim_patient_files = [f for f in self.data_files if self.DIM_PATIENT in f]
        for dim_file in dim_patient_files:
            if self.is_empty(dim_file):
                print("{} is empty.".format(dim_file))
                return False
        return True

    def is_empty(self, file_path):
        if file_path.startswith("gs://"):
            try:
                cmd = f"gsutil cat {file_path} | wc -l"
                line_count = int(subprocess.check_output(cmd, shell=True).decode().strip())
            except subprocess.CalledProcessError:
                line_count = 0
            return line_count == 0

        for i, line in enumerate(self.open_file(file_path)):
            if i == 0:
                continue
            return len(line) == 0

        return True

    def update_run_data_files(self):
        self.update_file_list("data_files.txt", self.data_files)
        self.update_file_list("dim_files.txt", self.dim_files)
        self.update_file_list("transfer_files.txt", self.transfer_files)

    def update_file_list(self, file_name, file_list):
        file_list = [f"{i[0]}{PIPE_SEP}{i[1]}" for i in file_list if len(i) > 1]
        file_path = os.path.join(self.RUN_STATE_DIR, self.RUN_VERSION, file_name)
        self.write_lines(file_path, file_list)
        print("Updated {}".format(file_path))

    def encode_version(self, major, minor):
        return "{0}.{1}".format(major, str(minor).zfill(3))

    def decode_version(self, run_version):
        return int(run_version.split(".")[0]), int(run_version.split(".")[1])

    def create_run_version(self, version, trigger):
        print("Last Version:", self.RUN_VERSION)
        if self.RUN_VERSION is None:
            if self.RUN_MODE == RunMode.Delta.value:
                raise Exception("No last full run found.")
            else:
                major_version = self.SOURCE_DATA_VERSION
                minor_version = 0
                self.RUN_VERSION = self.encode_version(major_version, minor_version)
        else:
            if self.run_state_log[self.RUN_VERSION]["STATUS"] != RunStatus.Success.value and not self.SAMPLE_RUN:
                raise Exception("Last run still active, please revive it carefully.")
            elif version is not None:
                self.RUN_VERSION = version
            else:
                major_version, minor_version = self.decode_version(self.RUN_VERSION)
                minor_version += 1
                self.RUN_VERSION = self.encode_version(major_version, minor_version)

        self.DATA_DIR = os.path.join(self.ROOT_DIR, self.RUN_VERSION)
        self.run_state_log.setdefault(self.RUN_VERSION, {
            "RUN_MODE": self.RUN_MODE,
            "START_DTM": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "END_DTM": "",
            "STATUS": RunStatus.Running.value,
            "RUN_SOURCE": "DEID_DELTA_UPDATE" if trigger is None else trigger
        })

    def create_version_dirs(self):
        version_in_progress_file = os.path.join(self.ROOT_DIR, self.RUN_VERSION, "version.inprogress")
        self.write_lines(version_in_progress_file)

    def release_version(self):
        self.run_state_log = self.load_run_version_state()
        print(f"Updating {self.RUN_VERSION} as {RunStatus.Success.value}")
        self.run_state_log[self.RUN_VERSION]["END_DTM"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.run_state_log[self.RUN_VERSION]["STATUS"] = RunStatus.Success.value
        self.update_run_version_state()
        self.write_lines(os.path.join(self.DATA_DIR, "version.done"), [])
        self.rmtree(os.path.join(self.DATA_DIR, "version.inprogress"))


if __name__ == "__main__":
    RunManager().run()
