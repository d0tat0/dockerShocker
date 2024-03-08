import os
import subprocess
import json
import csv
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
import logging

log = logging.getLogger(__name__)


def run_command(command):
    """
    return output,error,return code
    """
    result = subprocess.run(command, encoding='utf8', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return result.stdout, result.stderr, result.returncode


def check_if_file_exists(path):
    _, _, status = run_command(f'gsutil ls {path}')
    return status == 0


def read_csv_from_string(csv_string):
    reader = csv.reader(csv_string.splitlines(), delimiter='|')
    rows = [row for row in reader]
    return rows


class DataManager:
    def _load_pre_processed_versions(self):
        op, err, rc = run_command(f"gsutil ls {self.config['pre_processed_path']}")

        if rc == 0:
            arr = [item.split('/')[-2] for item in op.splitlines()]
            return sorted(arr, reverse=True)
        else:
            log.warning('pre processed path processing error')
            raise Exception

    def _check_if_valid_version(self, prefix):
        return prefix.startswith('5') and len(prefix) == 5

    def _load_datagen_versions(self):
        op, err, rc = run_command(f"gsutil ls {self.config['orchestrator_base_bucket']}")

        if rc == 0:
            arr = [item.split('/')[-2] for item in op.splitlines()]
            na = []
            for i in arr:
                if self._check_if_valid_version(i):
                    na.append(i)

            return sorted(na, reverse=True)
        else:
            log.warning('Datagen path processing error')
            raise Exception

    # Not used
    def _load_run_versions_txt(self):
        """
        5.000|FULL|2022-10-30 07:07:52|2022-11-02 13:46:25|SUCCESS|MAYO_FULL_UPDATE
        5.002|DELTA|2023-01-05 14:28:00|2023-01-13 06:09:53|SUCCESS|EES_UPDATE
        5.003|DELTA|2023-01-14 12:18:00||RUNNING|MAYO_DELTA_UPDATE_20230106


        Returns a list of DICT containing various property of that
        version
        sorted by latest to last version
        """
        rv_str, _, returncode = run_command(f"gsutil cat {self.config['run_versions_file_path']}")
        if returncode != 0:
            log.warning(f'Error in reading run versions from {self.config["run_versions_file_path"]}')
            raise Exception

        csv_arr = read_csv_from_string(rv_str)
        lod = []
        for item in reversed(csv_arr):
            if len(item) > 0:
                lod.append({
                    'version': item[0],
                    'run_mode': item[1],
                    'status': item[4]
                }
                )
        log.warning("------------------------------------")
        for d in lod:
            log.warning(d)
        log.warning("------------------------------------")

        return lod

    def _adjust_shuffle_partitions(self, source):
        self.num_files = 1000
        if source in ["FACT_ORDERS", "FACT_FLOWSHEETS", "FACT_CLINICAL_DOCUMENTS"]:
            self.SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", self.num_files * 5)
        else:
            self.SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", self.num_files)

    def init_spark_session(self, source=None):
        if self.SPARK_SESSION is not None:
            return self.SPARK_SESSION

        module_name = os.path.basename(sys.argv[0]).split(".")[0]
        run = "job"
        self.SPARK_SESSION = SparkSession.builder.appName(f"{module_name}_{run}").getOrCreate()
        self.SPARK_SESSION.conf.set("spark.sql.files.maxPartitionBytes", "1gb")
        self.SPARK_SESSION.conf.set("spark.sql.broadcastTimeout", 60 * 60)
        self.SPARK_SESSION.sparkContext.setLogLevel("WARN")
        self._adjust_shuffle_partitions(source)
        return self.SPARK_SESSION

    def __init__(self,
                 config_file=None,
                 tag=None,
                 version=None,
                 orchestrator_mode=False,
                 dry_run=False, sample_run=None):

        if config_file:
            self.config = json.load(open(config_file))
        else:
            import os
            dir_path = os.path.dirname(os.path.realpath(__file__))
            _config = 'sample_config.json' if sample_run else 'config.json'
            self.config = json.load(open(os.path.join(dir_path, _config)))
            if sample_run:
                self.config['pre_processed_path'] = os.path.join(self.config['pre_processed_path'].replace('PRE_PROCESSED', 'SAMPLE_PRE_PROCESSED'), sample_run)
                self.config['orchestrator_base_bucket'] = os.path.join(self.config['orchestrator_base_bucket'], sample_run)
        self.dry_run = dry_run
        self.pre_processed_versions = self._load_pre_processed_versions()
        log.warning(f"Pre processed versions: {self.pre_processed_versions}")
        self.datagen_versions = self._load_datagen_versions()
        log.warning(f"DATAGEN versions: {self.datagen_versions}")

        # self.rv = self._load_run_versions_txt()
        # self.release_versions = self._load_release_versions_txt()
        # self.harmonization_details = self._load_harmonized_tables_state()

        if version is not None:
            self.version = version
        else:
            self.version = self.datagen_versions[0]

        # configure self read bucket
        self.tag = tag
        self.orchestrator_mode = orchestrator_mode
        self.SPARK_SESSION = None

    def read_source_for_extraction(self, spark: SparkSession, source: str, source_fields: List[str],
                                   read_sample_only=False) -> DataFrame:

        pass

    def read_pre_processed(self, source: str, version: str, columns=None):
        path = os.path.join(self.config['pre_processed_path'], version, source.upper())
        log.warning(f'Reading pre processed folder {path}')
        if self.dry_run:
            return None

        if not self.SPARK_SESSION:
            spark = self.init_spark_session()
        else:
            spark = self.SPARK_SESSION

        if columns:
            return spark.read.parquet(path).select(columns)
        return spark.read.parquet(path)

    def read_datagen_data(self, source: str, version: str, columns=None):

        if source.lower().startswith('fact_') or source.lower() == 'dim_patient':
            folder_name = "FACT_TABLES"
        elif source.lower().startswith('dim_'):
            folder_name = "DIM_TABLES"
        else:
            raise NotImplementedError

        found = False
        for avl_version in self.datagen_versions:
            path = os.path.join(self.config['orchestrator_base_bucket'], avl_version, "DELTA_TABLES", folder_name,
                                source.upper() + ".parquet")

            log.warning(f'checking for {source} in {path}')
            if float(avl_version) > float(version):
                log.warning(f'skipping {avl_version} as it is higher version')
                continue

            if not check_if_file_exists(path):
                log.warning(f'skipping {avl_version} as the file in path: {path} does not exist')
                continue
            else:
                found = True
                log.warning(f'Found file in {path}')
                break


        if not found:
            log.warning(f'{source} not found in {self.datagen_versions}')
            raise Exception
        if self.dry_run:
            return None

        log.warning(f'Reading datagen folder {path}')
        if not self.SPARK_SESSION:
            spark = self.init_spark_session()
        else:
            spark = self.SPARK_SESSION
        if columns:
            return spark.read.parquet(path).select(columns)
        return spark.read.parquet(path)

    def read_data_source(self, source: str, columns: List[str] = None, version: str = None,
                         mode: str = "DELTA", latest=True):
        """
        returns the table for the specific version if applicable
        """
        orchestrator_version_list = self.datagen_versions

        version_to_get = self.version
        if version is not None and (version in orchestrator_version_list or version in self.pre_processed_versions):
            version_to_get = version
        else:
            log.warning(f'Provided version: {version} for table {source} does not exist. ')
            raise Exception

        return_from_pre_processed = False
        if version_to_get in self.pre_processed_versions and version_to_get not in orchestrator_version_list and source.lower().startswith(
                "fact_") and mode == "DELTA":
            return_from_pre_processed = True

        if return_from_pre_processed:
            df = self.read_pre_processed(source, version_to_get, columns)
            return df

        if version_to_get in orchestrator_version_list:
            return self.read_datagen_data(source, version_to_get, columns)

        log.warning(f'The specified version: {version_to_get} for table: {source} with read mode: {mode} is not valid\n returning None')
        return None


def run_testcases():
    dh = DataManager(dry_run=True)
    version_list = ['5.000', '5.002', '5.003','5.007']
    table_list = ['FACT_LAB_TEST', 'DIM_PATIENT', 'FACT_PROCEDURES', 'DIM_SOURCE_SYSTEM']
    for version in version_list:
        for table in table_list:
            log.warning(f'-----------processing {version} {table} ------------')
            try:
                df = dh.read_data_source(table, version=version)
                df.show(3)
            except Exception as e:
                log.warning(e)
                log.warning(f"Read for {table}.{version} failed")


if __name__ == "__main__":
    run_testcases()
