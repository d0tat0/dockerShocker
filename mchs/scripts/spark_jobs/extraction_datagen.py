import json
import os
import time
from typing import Dict, List

from pyspark.sql import *
from pyspark.sql import functions as F

from spark_jobs.orchestrator import Orchestrator
from spark_ees_datagen.data_manager import DataManager

SAMPLE_SIZE = 10000
FULL = "FULL"
EES_UPDATE = "EES_UPDATE"
DELTA = "EES_DELTA"


def get_current_time_str(format=None):
    if format is None:
        return time.strftime("%Y.%m.%d_%H.%M.%S")
    else:
        return time.strftime(format)


def symlink_folder_to_latest(folder_name, is_ees_version):
    parent = os.path.abspath(os.path.join(folder_name, os.pardir))
    dest = os.path.join(parent, 'latest')

    try:
        print(f"Making a symlink from {dest} to {folder_name}")
        os.symlink(folder_name, dest, target_is_directory=True)
    except:
        print("Making symlink  not work")


def rm_folder(folder_name):
    try:
        print(f"Removing symlink")
        os.remove(folder_name)
    except:
        print(f"removing symlink: {folder_name} Did not work. Maybe its not present.")


class ExtractionJob(Orchestrator):

    def __init__(self, harmonization_source_table, config_file=None,
                 debug=False,
                 testing=False):
        print("****************************** Running latest spark dev ********************************")
        if debug:
            print("*** RUNNING with debug on. Will operate on sample data. ***")

        self.debug = debug
        self.testing = testing
        self.cloud_read = False
        if self.cloud_read:
            self.dm = DataManager(harmonization_source_table, sample_run=self.SAMPLE_GT)

        super().__init__(config_file)
        if self.ees_full_update():
            self.EES_RUN_MODE = "EES_FULL"
        else:
            self.EES_RUN_MODE = "EES_DELTA"

        self.creds_file_path = os.path.join(self.SYSTEM_DICT["ees_resources_dir"],
                                            self.SYSTEM_DICT["extraction_jobs"]["creds_file"])

        print(f"{self.creds_file_path}")
        self.harmonization_table = harmonization_source_table
        self.ext_config = self.SYSTEM_DICT["extraction_jobs"]["tables"][harmonization_source_table]["extractions"]
        self.harmonized_root_dir_name = self.SYSTEM_DICT["extraction_jobs"]["output_root_dir"]  # harmonized interim
        self.meta_dir_name = self.SYSTEM_DICT["extraction_jobs"]["output_meta_dir"]
        self.final_output_dir_name = self.SYSTEM_DICT["extraction_jobs"]["final_output_dir"]
        self.omop_vocab_dir = self.SYSTEM_DICT["extraction_jobs"]["omop_vocab_dir"]

        run_dir_path = os.path.join(self.harmonized_root_dir_name, self.harmonization_table.upper())
        self.harmonized_root_folder_path = os.path.join(self.LOCAL_DATA_DIR, run_dir_path)
        self.meta_dir_path = os.path.join(self.harmonized_root_folder_path, self.meta_dir_name)
        self.log_folder = os.path.join(self.harmonized_root_folder_path, "LOGS")
        for dir in [self.harmonized_root_folder_path, self.meta_dir_path, self.log_folder]:
            os.makedirs(dir, exist_ok=True)
            print(f"Created {dir}")

        self.final_harmonized_root_dir = os.path.join(self.DATA_DIR, run_dir_path)
        self.final_meta_dir = os.path.join(self.final_harmonized_root_dir, self.meta_dir_name)
        self.final_log_dir = os.path.join(self.final_harmonized_root_dir, "LOGS")
        print("FINAL_DIRS: ", "\n".join([self.final_harmonized_root_dir, self.final_meta_dir, self.final_log_dir]))

        self.stamp_orch_meta_file(harmonization_source_table)
        self.create_log_folder()
        print(f"** Running on {self.EES_RUN_MODE}")

    def get_available_harmonization_tag_for_source_table(self, harmonization_source_table):
        try:
            with open(self.harmonisation_config_file, 'r') as fp:
                cfg = json.load(fp)
                for key, table_config in cfg['jobs'].items():
                    if table_config["id"] == self.SYSTEM_DICT["extraction_jobs"]["tables"][harmonization_source_table][
                        "id"]:
                        tag = table_config["tag"]
        except:
            print(f"""Error in reading {self.harmonisation_config_file}.
                Unable to fetch the tag. write manually at
                {os.path.join(self.meta_dir_path, self.SYSTEM_DICT["extraction_jobs"]["orchestrator_meta_file"])}""")
            tag = "NA"
        return tag

    def stamp_orch_meta_file(self, harmonization_source_table):
        self.meta_content = {
            "ees_run_mode": self.EES_RUN_MODE,
            "run_mode": self.RUN_MODE,
            "version": self.RUN_VERSION,
            "tag": self.get_available_harmonization_tag_for_source_table(harmonization_source_table)
        }
        try:
            print("Writing meta file with run information")
            outfile_path = os.path.join(self.final_meta_dir,
                                        self.SYSTEM_DICT["extraction_jobs"]["orchestrator_meta_file"])
            self.dump_json_data(outfile_path, self.meta_content)
        except Exception as e:
            print(e)
            print('-----------------------------------')
            print("Not able to write meta.json. Raising Exception. You can manually write.")
            print(self.meta_content)

    def create_log_folder(self):
        print("Creating log folder {}".format(self.log_folder))
        os.system("mkdir -p {}".format(self.log_folder))


    def read_source_for_extraction(self, spark: SparkSession, source: str, source_fields: List[str],
                                   read_sample_only=False) -> DataFrame:
        source = source.upper()
        if not read_sample_only:
            read_sample_only = self.debug
        print(f"self.debug = {self.debug}")
        print(f"read_sample_only {read_sample_only}")

        if self.cloud_read:
            version = self.RUN_VERSION
            df = self.dm.read_data_source(source, source_fields, version)
            if read_sample_only:
                df = df.limit(SAMPLE_SIZE)
        else:
            if source.startswith("FACT_"):
                df = self.read_fact_source_for_ees(spark, source, source_fields, read_sample_only)
                print(f'Returned df size: {df.count()}')
                return df
            df = self.read_versioned_dim_table(source).select(source_fields)
        print(f'Returned df size: {df.count()}')
        return df


    def write_extracted_data(self, extraction_df: DataFrame, extraction_name: str, version_meta: dict = None,
                             stats: dict = None):
        out_dir = os.path.join(self.final_harmonized_root_dir, self.final_output_dir_name, extraction_name.upper())
        columns = self.ext_config[extraction_name.upper()]["extraction_schema"]
        print("Writing data to {}".format(out_dir))
        self.write_df_to_delta_lake(extraction_df, out_dir, columns)
        if version_meta:
            file_name = "{}_version_meta.json".format(extraction_name)
            self.dump_json_data(os.path.join(self.final_log_dir, file_name), version_meta)
        if stats:
            file_name = "{}_stats.json".format(extraction_name)
            self.dump_json_data(os.path.join(self.final_log_dir, file_name), stats)

    def read_fact_source_for_ees(self, spark: SparkSession,
                                 source,
                                 fact_fields,
                                 run_mode,
                                 read_sample_only=None,
                                 ) -> DataFrame:
        if read_sample_only is None:
            read_sample_only = self.debug

        read_schema = self.read_schema_for_ees(source, fact_fields)
        print(f'read_fact_source_for_ees.read_sample_only {read_sample_only}')

        if self.EES_RUN_MODE == "EES_DELTA":
            try:
                df = self.read_incremental_raw_data(source)
                df = df.select(read_schema)
                if read_sample_only:
                    return df.limit(SAMPLE_SIZE)
                return df
            except:
                print(f'Unable to read the RAW data.. falling back to datagen data')
        # Read from datagen data when its EES_UPDATE
        df = self.read_versioned_datagen_dir(source, columns=read_schema)
        if read_sample_only:
            return df.limit(SAMPLE_SIZE)
        return df



    def read_schema_for_ees(self, source, fact_fields=[]):
        # columns of fact_fields should be present in both raw data and orchestrator output
        source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
        nfer_schema = source_config["nfer_schema"]
        amc_schema = source_config["amc_schema"]
        dim_syn_dk_cols = self.read_dim_syn_dk_schema(source)
        
        for column in fact_fields:
            if column != "NFER_PID" and (column not in nfer_schema or column not in amc_schema) and (column not in dim_syn_dk_cols):
                print("{} not present in source".format(column))
                raise Exception("Some columns are not present in either raw_data or orchestrator output")
        return fact_fields


    def last_processed_tag(self, source):
        stats_file = self.last_data_dir("DATAGEN", "STATS", f"{source.lower()}.json")
        if not stats_file:
            print("Could not find stats file for {}".format(source))
            return None
        stats = self.get_json_data(stats_file)
        return stats.get("extraction_tag")


    def get_previous_harmonized_meta_table(self, spark: SparkSession, table_name):
        paths = [self.harmonized_root_dir_name, self.harmonization_table.upper(),
                 self.meta_dir_name, table_name.upper()]
        harmonized_meta_dir = self.last_data_dir(*paths)
        if not harmonized_meta_dir:
            paths = [self.harmonized_root_dir_name, self.harmonization_table.upper(), 'latest',
                     self.meta_dir_name, table_name.upper()]
            harmonized_meta_dir = self.last_data_dir(*paths)
        if harmonized_meta_dir:
            print("Reading from {}".format(harmonized_meta_dir))
            meta_df = self.read_delta_to_df(harmonized_meta_dir)
            return meta_df
        else:
            return None


    def get_previous_extracted_data(self, spark: SparkSession, extraction_name):
        paths = [self.harmonized_root_dir_name, self.harmonization_table.upper(),
                 self.final_output_dir_name, extraction_name.upper()]
        extraction_df_dir = self.last_data_dir(*paths)
        if not extraction_df_dir:
            paths = [self.harmonized_root_dir_name, self.harmonization_table.upper(), 'latest',
                     self.final_output_dir_name, extraction_name.upper()]
            extraction_df_dir = self.last_data_dir(*paths)
        if extraction_df_dir:
            print("Reading from {}".format(extraction_df_dir))
            extraction_df = self.read_delta_to_df(extraction_df_dir)
            return extraction_df
        else:
            print("No previous extractions found for {}".format())
            return None


    def write_harmonized_meta_table(self, meta_df, table_name):
        harmonized_meta_dir = os.path.join(self.final_meta_dir, table_name.upper())
        columns = meta_df.columns
        print("Writing metadata to {}".format(harmonized_meta_dir))
        self.write_df_to_delta_lake(meta_df, harmonized_meta_dir, columns)


    def read_omop_vocabulary(self, spark: SparkSession, vocab_name):
        root_dir = self.ROOT_DIR
        if self.SAMPLE_RUN:
            root_dir = self.ROOT_DIR.replace(self.SAMPLE_GT, "")
        vocab_dir_path = os.path.join(root_dir, self.omop_vocab_dir, vocab_name)

        vocab_df = self.read_delta_to_df(vocab_dir_path)
        return vocab_df

    # To be removed when this functionality is included in data manager
    def read_amc_source_concept_meta(self):
        # TODO Correct the path after testing
        out_dir = os.path.join(self.DATA_DIR, "PRE_OMOP_DATA", "SOURCE_CONCEPT_META")
        data_df = self.read_parquet_to_df(out_dir)
        return data_df

def test_bucket_reading():
    table_column_dict = {
        "FACT_ORDERS": ["ORDER_ROUTE_DESCRIPTION", "ORDER_FREQUENCY", "ORDER_ITEM_DK", "ORDER_DOSE_AMOUNT",
                        "ORDER_STRENGTH", "MED_GENERIC",
                        "ORDER_FORM_DESCRIPTION", ],
        "dim_order_item": ["ORDER_ITEM_DK", "MEDICATION_GENERIC_NAME", "ORDER_DESCRIPTION"],

        "FACT_DIAGNOSIS": ["NFER_PID", "DIAGNOSIS_CODE_DK"],
        "dim_diagnosis_code": ["DIAGNOSIS_CODE", "DIAGNOSIS_CODE_DK", "DIAGNOSIS_DESCRIPTION", "DIAGNOSIS_METHOD_CODE"],
        "FACT_MEDS_ADMINISTERED": ["MED_NAME_DK", "ADMINISTERED_FREQUENCY", "ADMINISTERED_QUANTITY"],
        "DIM_MED_NAME": ["MED_NAME_DK"],
        "DIM_MEDICATION_TO_NDC_MAPPING": ["MED_NAME_DK"],
        "fact_procedures": ["PROCEDURE_CODE_DK", ],
        "dim_procedure_code": [
            'PROCEDURE_CODE_DK',
            'PROCEDURE_DESCRIPTION',
            'PROCEDURE_CODE',
            'PROCEDURE_METHOD_CODE'
        ],
    }
    """
        "FACT_LAB_TEST":[],
        "dim_lab_panel_test_code":[],
        "dim_lab_test_code":[],
        "FACT_ECHO_RESULTS":[],
        "dim_echo_result_type":[],
        "dim_flowsheet_row_name":[]
    }
    """
    e = ExtractionJob(harmonization_source_table="FACT_ORDERS")
    spark = e.init_spark_session("FACT_ORDERS", )

    all_versions = ["5.000", "5.002", "5.003", "5.005", "5.007"]

    for source in table_column_dict:
        for version in all_versions:
            print("--------------------------------------------------")
            print(f"Comparing read for {source} {version}")

            e.RUN_VERSION = version
            e.cloud_read = False
            failed = False

            try:
                df_legacy = e.read_source_for_extraction(spark, source, table_column_dict[source])
            except Exception as e:
                failed = True
                print('Legacy read has failed')
                print(e)

            e.cloud_read = True
            try:
                df_curr = e.read_source_for_extraction(spark, source, table_column_dict[source])
            except Exception as e:
                failed = True
                print('cloud read has failed')
                print(e)
            if failed:
                continue
            print('legacy')
            df_legacy.show(4)
            print('cloud')
            df_curr.show(4)
            if df_curr is None or df_legacy is None:
                print(f'One of the dataframes is null')

                continue

            c1 = df_legacy.count()
            c2 = df_curr.count()
            if c1 != c2:
                print(f"Count mismatch between two dfs, legacy: {c1} current: {c2}")
            if df_legacy.columns != df_curr.columns:
                print(f"Column name mismatch legacy: {df_legacy.columns} vs {df_curr.columns}")
            print("--------------------------------------------------")


def test_labtest_reading():
    print(f'Testing labtest reading')

    source = "FACT_ORDERS"
    REQUIRED_FIELDS = ["ORDER_ITEM_DK"]

    testcases = ["FULL_MODE_SAMPLE_OFF",
                 "DELTA_MODE_SAMPLE_OFF",
                 "FULL_MODE_SAMPLE_ON",
                 "DELTA_MODE_SAMPLE_ON", ]
    e = ExtractionJob(harmonization_source_table=source, debug=True)
    spark = e.init_spark_session("FACT_ORDERS", )

    for idx, case in enumerate(testcases):

        print("----------------------------------------")
        print(case)
        print("----------------------------------------")

        if idx == 0:
            try:
                df = e.read_fact_source_for_ees(spark, source, REQUIRED_FIELDS, run_mode="FULL", read_sample_only=False)
                df.show()
            except Exception as e:
                print(e)
                print(case, "Failed")
        elif idx == 1:
            try:
                df = e.read_fact_source_for_ees(spark, source, REQUIRED_FIELDS, run_mode="DELTA",
                                                read_sample_only=False)
                df.show()
            except Exception as e:
                print(e)
                print(case, "Failed")
        elif idx == 2:
            try:
                df = e.read_fact_source_for_ees(spark, source, REQUIRED_FIELDS, run_mode="FULL", read_sample_only=True)
                df.show()
            except Exception as e:
                print(e)
                print(case, "Failed")

        elif idx == 3:
            try:
                df = e.read_fact_source_for_ees(spark, source, REQUIRED_FIELDS, run_mode="DELTA", read_sample_only=True)
                df.show()
            except Exception as e:
                print(e)
                print(case, "Failed")

if __name__ == "__main__":
    test_bucket_reading()
