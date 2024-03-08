import os
import json
from multiprocessing import Pool
from datetime import datetime

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import *
from spark_jobs.schema_manager import SchemaManager
from core.config_vars import PIPE_SEP, MAX_PROCESS, DIR_DTM_FORMAT

# This message is set when the version & updated_by is set to "null"
HIVE_DEFAULT_PARTITION_MESSAGE = "VERSION=__HIVE_DEFAULT_PARTITION__/UPDATED_BY=__HIVE_DEFAULT_PARTITION__"


class DataWriter(SchemaManager):

    def __init__(self, config_file = None):
        super().__init__(config_file)

    def timestamp_dir(self, dir_path):
        file_name = f"_{datetime.now().strftime(DIR_DTM_FORMAT)}"
        file_path = os.path.join(dir_path, file_name)
        self.write_lines(file_path)

    def write_dim_final(self, dim_df: DataFrame, dim_source):
        dim_config = self.SYSTEM_DICT["info_tables"][dim_source.lower()]
        nfer_schema = dim_config["nfer_schema"]
        dim_file_path = os.path.join(self.DATAGEN_DIR, "DIM_INFO", dim_config["files"][0])
        if len(nfer_schema):
            nfer_schema = nfer_schema + ["VERSION", "UPDATED_BY"]
            dim_df = dim_df.select(nfer_schema)
        if self.IS_CSV_MODE:
            self.write_df_to_csv(dim_df, dim_file_path)
        else:
            self.df_to_parquet(dim_df, dim_file_path)

    def overwrite_latest_delta(self, current_path, latest_path):
        if self.glob(latest_path):
            self.rmtree(latest_path)
        self.copy_directory(current_path, latest_path)

    def write_df_to_delta_lake(self, source_df: DataFrame, out_dir, nfer_schema=None, latest_delta_dir=None):
        if self.glob(out_dir):
            self.rmtree(out_dir)
        if nfer_schema:
            source_df = source_df.select(nfer_schema)
        partitionBy = "VERSION" in source_df.columns and "UPDATED_BY" in source_df.columns
        df_writer = source_df.write.partitionBy("VERSION", "UPDATED_BY") if partitionBy else source_df.write
        print(f"Writing to {out_dir}")
        df_writer.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_dir)
        self.timestamp_dir(out_dir)
        if latest_delta_dir:
            print(f"Overwriting latest delta: {latest_delta_dir}")
            self.overwrite_latest_delta(out_dir, latest_delta_dir)

    def release_versioned_delta_table(self, source_df:DataFrame, source: str, version=None, root_dir=None, write_latest_delta=False):
        if version is None:
            version = self.RUN_VERSION

        # Add syn_dk columns
        source_df = self.add_syn_dk_col(source_df, source)

        info_files_list = []
        for key, value in self.SYSTEM_DICT["info_tables"].items():
            info_files_list.append(os.path.splitext(value["files"][0].lower())[0])

        source_type = "META_TABLES"
        if source.startswith("FACT_SYN_") or source.upper().startswith("DIM_SYN") or source in ["FACT_HARMONIZED_MEASUREMENTS", "FACT_AUGMENTED_CURATION"]:
            source_type = "SYN_TABLES"
        elif source.lower() in [table.lower() for table in list(self.SYSTEM_DICT["data_tables"].keys())]:
            source_type = "FACT_TABLES"
        elif source.lower() in [table.lower() for table in list(self.SYSTEM_DICT["info_tables"].keys())] or source.lower() in info_files_list:
            source_type = "DIM_TABLES"

        if not root_dir:
            root_dir = self.WRITE_ROOT_DIR

        source = source.upper()

        delta_table_path = os.path.join(root_dir, version, "DELTA_TABLES", source_type, f"{source.upper()}.parquet")
        if self.skip_if_exists and self.glob(os.path.join(delta_table_path, "_delta_log/*.json")):
            print(f"Already exists, hence skipping {delta_table_path}")
            return delta_table_path

        data_count = None
        if source_type == "FACT_TABLES":
            fact_maps_stats_file = os.path.join(root_dir, version, "FACT_MAPS_VERSIONED", "STATS", f"{source}.json".lower())
            if self.glob(fact_maps_stats_file):
                fact_maps_stats = self.get_json_data(fact_maps_stats_file)
                data_count = fact_maps_stats.get("fact_rows_all_versions")

        if data_count is None:
            source_df.cache()
            data_count = source_df.count()
            print(f"{source} DATA_COUNT = {data_count}")

        latest_delta_dir = None
        if write_latest_delta:
            latest_delta_dir = os.path.join(root_dir, self.LATEST_DELTA_DIR, "DELTA_TABLES", source_type, f"{source.upper()}.parquet")

        print(f'Writing {source} to {delta_table_path}')
        self.write_df_to_delta_lake(source_df, delta_table_path)
        source_df.unpersist()

        success, error = self.validate_delta_table(delta_table_path, version, source_type, data_count)
        if not success:
            raise Exception(error)
        return delta_table_path

    def validate_delta_table(self, delta_table_path, version, source_type, data_count):
        delta_table = os.path.basename(delta_table_path)
        delta_logs = self.glob(os.path.join(delta_table_path, "_delta_log/*.json"))
        if not delta_logs:
            return False, f"{delta_table} does not contain _delta_log"
        if self.glob(os.path.join(delta_table_path, "VERSION=__HIVE_DEFAULT_PARTITION__")):
            print(f"{delta_table} contains __HIVE_DEFAULT_PARTITION__")

        delta_count = None
        # For counting records with HIVE_DEFAULT_PARTITION_MESSAGE
        null_records_count = 0
        for line in self.open_file(delta_logs[0]):
            if "numOutputRows" in line:
                delta_log = json.loads(line)
                delta_count = int(delta_log["commitInfo"]["operationMetrics"]["numOutputRows"])
            elif HIVE_DEFAULT_PARTITION_MESSAGE in line:
                null_records_count += 1

        if delta_count is None:
            return False, f"Failed to retrieve count from delta table {delta_table}"
        # data_count comes from stats; currently doesn't include null records, so adding here
        elif delta_count != (data_count + null_records_count):
            return False, f"{delta_table} DELTA_COUNT = {delta_count} != ROW_COUNT = {data_count + null_records_count}"

        return True, None

    def create_patient_blocks(self, source_df: DataFrame, source, nfer_schema, order_by):
        if source is None:
            source = self.DIM_PATIENT

        # FILE PAR PARTITION
        self.adjust_shuffle_partitions(source in self.BIG_TABLES)
        if "FILE_ID" not in source_df.columns:
            source_df = self.assign_file_id_for_nfer_pid(source_df, source)
        nfer_schema = [n for n in nfer_schema if n != "FILE_ID"]
        source_df = source_df.repartition("FILE_ID")

        # ROW_ID GENERATION
        if "ROW_ID" not in source_df.columns and "ROW_ID" in nfer_schema:
            source_df = self.generate_row_id(source_df, order_by=order_by)

        # SORT BY ROW_ID in FINAL
        source_df = source_df.select(["FILE_ID"] + nfer_schema)
        if "ROW_ID" in source_df.columns:
            source_df = source_df.sortWithinPartitions("ROW_ID")

        return source_df

    def write_final_dir(self, dir_name, source_df: DataFrame, nfer_schema, source=None, order_by=None,version=None):
        if not version:
            data_dir = self.DATA_DIR
        else:
            data_dir = os.path.join(self.WRITE_ROOT_DIR,version)
        final_dir = os.path.join(data_dir, f"{dir_name}.parquet", source)
        self.df_to_parquet_blocks(source_df, final_dir, nfer_schema, source, order_by)

    def df_to_parquet_blocks(self, source_df: DataFrame, final_dir: str, nfer_schema, source=None, order_by=None):
        final_df = self.create_patient_blocks(source_df, source, nfer_schema, order_by)
        self.df_to_parquet(final_df, final_dir, nfer_schema)

    def df_to_patient_blocks(self, source_df: DataFrame, final_dir: str, nfer_schema, source=None, order_by=None) -> None:
        final_df = self.create_patient_blocks(source_df, source, nfer_schema, order_by)
        out_dir = os.path.join(final_dir, "_final_tmp")
        final_df.write.partitionBy("FILE_ID").option("emptyValue", None).csv(
            out_dir,
            sep=self.csv_separator,
            header=False,
            mode="overwrite",
            emptyValue='',
            nullValue='',
            quote=""
        )
        # CONVERT TO patient_*.final
        final_file_count = self.rename_to_patient_blocks(final_dir, out_dir, source)
        nfer_schema, type_schema = self.extract_df_schema(final_df)
        self.create_headers(final_dir, nfer_schema, type_schema)
        self.timestamp_dir(final_dir)
        print(f"{final_file_count} patient_*.final generated at {final_dir}")

    def df_to_parquet(self, source_df: DataFrame, out_dir, out_schema=None):
        if out_schema:
            source_df = source_df.select(out_schema)
        print(f"Writing {out_dir}")
        source_df.write.parquet(out_dir, mode="overwrite", compression="snappy")
        self.timestamp_dir(out_dir)

    def write_df(self, source_df: DataFrame, final_dir, nfer_schema) -> None:
        source_df = source_df.select(nfer_schema)
        source_df.write.option("emptyValue", None).csv(
            final_dir,
            sep=self.csv_separator,
            header=False,
            mode="overwrite",
            emptyValue='',
            nullValue='',
            quote=""
        )
        nfer_schema, type_schema = self.extract_df_schema(source_df)
        self.create_headers(final_dir, nfer_schema, type_schema)
        self.timestamp_dir(final_dir)

    def generate_row_id(self, source_df: DataFrame, order_by=None) -> DataFrame:
        if order_by:
            window = Window.partitionBy("FILE_ID").orderBy(order_by)
        elif "NFER_PID" in source_df.columns and "NFER_DTM" in source_df.columns:
            window = Window.partitionBy("FILE_ID").orderBy("NFER_PID", "NFER_DTM")
        elif "NFER_PID" in source_df.columns:
            window = Window.partitionBy("FILE_ID").orderBy("NFER_PID")
        else:
            window = Window.partitionBy("FILE_ID").orderBy("FILE_ID")
        source_df = source_df.withColumn("ROW_ID", F.row_number().over(window))
        return source_df

    def rename_to_patient_blocks(self, final_dir, out_dir, source):
        self.rmtree(f"{final_dir}/patient_*.final")
        file_id_list = self.get_file_id_list(source)
        final_file_count = len(file_id_list)
        spark_session = self.SPARK_SESSION
        delattr(self, "SPARK_SESSION")
        pool = Pool(processes=MAX_PROCESS)
        pool.starmap(self.rename_patient_final, [(final_dir, out_dir, file_id) for file_id in file_id_list])
        pool.close()
        self.rmtree(out_dir)
        self.SPARK_SESSION = spark_session
        return final_file_count

    def rename_patient_final(self, final_dir, out_dir, file_id):
        dir_path = f"FILE_ID={file_id}"
        output_files = self.glob(os.path.join(out_dir, dir_path, f"part-*.csv"))
        file_count = len(output_files)
        final_file_name = self.get_patient_final_file_name(file_id)
        final_file_path = os.path.join(final_dir, final_file_name)
        if file_count == 0:
            self.write_lines(final_file_path)
        elif file_count == 1:
            self.rename_file(output_files[0], final_file_path)
        else:
            raise Exception(f"{file_count} files found for {file_id}, expected only 1.")

    def df_to_pre_processed_parquet(self, source, source_df: DataFrame, amc_schema, out_dir=None) -> None:
        unique_schema = self.SYSTEM_DICT['data_tables'][source.lower()]["unique_indices"]
        window = Window.partitionBy("FILE_ID").orderBy(unique_schema)
        source_df = source_df.withColumn("ROW_ID", F.row_number().over(window))
        if out_dir is None:
            out_dir = os.path.join(self.INPUT_SOURCE_DIR, source.upper())
        self.df_to_parquet(source_df, out_dir, ["FILE_ID"] + amc_schema)

    def write_df_to_csv(self, df: DataFrame, file_path, header=True):
        print("Writing data to {}".format(file_path))
        self.rmtree(file_path)
        temp_folder = os.path.join(os.path.dirname(file_path), "tmp_out")
        df.coalesce(1).write.csv(
            temp_folder,
            sep=self.csv_separator,
            header=header,
            mode="overwrite",
            emptyValue='',
            nullValue='',
            quote=""
        )
        output_files = self.glob(os.path.join(temp_folder, f"part-*.csv"))
        self.rename_file(output_files[0], file_path)
        self.rmtree(temp_folder)

    def log_spark_stats(self, data_dir_name, source, counter, version=None):
        root_dir = self.WRITE_ROOT_DIR if self.SAMPLE_RUN else self.ROOT_DIR
        version = version if version else self.RUN_VERSION
        data_dir_path = os.path.join(root_dir, version, data_dir_name)
        file_name = os.path.join(data_dir_path, "STATS", f"{source.lower()}.json")
        print(source,file_name,json.dumps(counter, indent=4))
        self.dump_json_data(file_name, counter)
