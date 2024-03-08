import os
from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, LongType, MapType, IntegerType
from pyspark.sql import Window


SOURCE_CONCEPT_META_CONFIG_FILE = "source_concept_meta.json"
SOURCE_CONCEPT_META_DIR = "SOURCE_CONCEPT_META"
PRE_OMOP_DATA_DIR = "PRE_OMOP_DATA"


class SourceConceptGenerator(Orchestrator):
    def __init__(self) -> None:
        super().__init__()
        self.load_source_concept_meta_config()
        self.init_spark_session()

    def parse_options_source(self):
        if self.options.run == "ALL" or self.options.source == "ALL":
            table_list =[table.strip() for table in self.AMC_SOURCE_CONCEPT_META_CONFIG["source_concept_tables"].keys()]
            return table_list
        else:
            if "," in self.options.source:
                return [table.strip() for table in self.options.source.split(",")]
            return [self.options.source]
        
    def set_options(self, parser):
        parser.add_option('--source_concept_write_path', default = None, help='Path to write the test output of source_concept_meta')
        parser.add_option('--dim_syn_read_path', default = None, help='Path to read the test output of dim_syn_interims')
        super().set_options(parser)

    def load_source_concept_meta_config(self):
        source_concept_meta_path = os.path.join(os.path.dirname(self.config_file), SOURCE_CONCEPT_META_CONFIG_FILE)
        self.AMC_SOURCE_CONCEPT_META_CONFIG = self.get_json_data(source_concept_meta_path)

    def create_empty_amc_source_concept_df(self):
        schema = StructType([
            StructField("SOURCE_CONCEPT_ID", LongType(), True),
            StructField("AMC_ID", LongType(), nullable=True),
            StructField("SOURCE_DESCRIPTION", StringType(), nullable=True),
            StructField("SOURCE_CODE", StringType(), nullable=True),
            StructField("SOURCE_VOCABULARY", StringType(), nullable=True),
            StructField("SOURCE_TABLE", StringType(), True),
            StructField("PATIENT_COUNT", IntegerType(), True),
            StructField("RECORD_COUNT", IntegerType(), True),
            StructField("META", StructType([
                StructField("DESCRIPTIONS", MapType(StringType(), StringType(), valueContainsNull=True), nullable=True),
                StructField("OTHER_FIELDS", MapType(StringType(), StringType(), valueContainsNull=True), nullable=True)
            ]), nullable=True)
        ])
        empty_df = self.SPARK_SESSION.createDataFrame([], schema)
        return empty_df

    def read_source_data_table(self, source, dk_col_name):
        read_columns = ["NFER_PID", dk_col_name]
        final_df = None
        if source in self.SYSTEM_DICT["syn_tables"]:
            return self.read_versioned_syn_data(source)
        if self.RUN_MODE == RunMode.Delta.value:
            # Read previous versions output and merge with current PRE_PROCESSED incremental data
            previous_version = self.VERSION_LIST[1]
            final_df = self.read_versioned_datagen_dir(source, columns=read_columns, version=previous_version)
            delta_df = self.read_incremental_raw_data(source, read_columns)
            if delta_df:
                final_df = final_df.unionByName(delta_df)
        elif self.RUN_MODE == RunMode.Full.value:
            if self.ees_full_update():
                # Read the previous versions full output
                previous_version = self.VERSION_LIST[1]
                final_df = self.read_versioned_datagen_dir(source, columns=read_columns, version=previous_version)
            else:
                # Base version run, read only PRE_PROCESSED output
                final_df = self.read_incremental_raw_data(source, read_columns)
        return final_df

    def add_map_column(self, source_df, col_name, source_columns):
        map_columns = []
        for column in source_columns:
            map_columns.extend([F.lit(column), F.col(column)])
        if len(map_columns) > 0:
            source_df = source_df.withColumn(col_name, F.create_map(map_columns))
        else:
            source_df = source_df.withColumn(col_name, F.create_map().cast("map<string,string>"))
        return source_df

    def add_patient_record_counts(self, source_df, transform_configs):
        data_table = transform_configs["data_table"]
        dk_col_name = transform_configs["data_table_amc_id_join_col_name"]
        data_df = self.read_source_data_table(data_table, dk_col_name)
        data_df = data_df.groupBy(dk_col_name).agg(F.count("NFER_PID").alias(
            "RECORD_COUNT"), F.countDistinct("NFER_PID").alias("PATIENT_COUNT"))
        data_df = data_df.withColumnRenamed(dk_col_name, "AMC_ID")
        source_df = source_df.join(F.broadcast(data_df), "AMC_ID", "left")
        return source_df

    def read_dim_table_for_source_concept_meta(self, source):
        if source.lower() in self.SYSTEM_DICT["info_tables"]:
            source_df = self.read_versioned_dim_table(source)
        else:
            print(f"Table {source} not found in info_tables list, searching in DIM_SYNS")
            if self.options.run == "TEST" and self.options.dim_syn_read_path:
                data_path = os.path.join(self.options.dim_syn_read_path, source.upper())
                if self.glob(data_path):
                    source_df = self.read_parquet_to_df(data_path)
                    return source_df
                else:
                    print(f"Can't find source in {data_path}, reading from orchestrator outputs")
                    source_df = self.read_latest_dim_syn(source)
                    return source_df
            source_df = self.read_latest_dim_syn(source)
        return source_df

    def generate_source_concept_meta(self, source_concept_table):
        transform_configs = self.AMC_SOURCE_CONCEPT_META_CONFIG["source_concept_tables"][source_concept_table]
        dim_source = transform_configs["source"]
        source_df = self.read_dim_table_for_source_concept_meta(dim_source)
        source_df = source_df.withColumn("SOURCE_TABLE", F.lit(dim_source.upper()))

        if transform_configs.get("column_transforms"):
            for column in transform_configs["column_transforms"]:
                source_df = source_df.withColumn(column, F.col(transform_configs["column_transforms"][column]))

        if transform_configs.get("constants"):
            for column in transform_configs["constants"]:
                source_df = source_df.withColumn(column, F.lit(transform_configs["constants"][column]))

        desc_columns = transform_configs["meta"]["descriptions"]
        other_fields = transform_configs["meta"]["other_fields"]

        source_df = self.add_map_column(source_df, "DESCRIPTIONS", desc_columns)
        source_df = self.add_map_column(source_df, "OTHER_FIELDS", other_fields)

        meta_columns = ["DESCRIPTIONS", "OTHER_FIELDS"]
        source_df = source_df.withColumn("META", F.struct(*meta_columns))


        source_df = self.add_patient_record_counts(source_df, transform_configs)

        final_columns = self.create_empty_amc_source_concept_df().drop("SOURCE_CONCEPT_ID").columns

        # Fill with null if there is no rule in config
        for column in final_columns:
            if column not in source_df.columns:
                source_df = source_df.withColumn(column, F.lit(None))

        source_df = source_df.select(final_columns)

        # Mark SOURCE_VOCABULARY as None when SOURCE_CODE is None
        # Happens for Duke due to structure of data
        source_df = source_df.withColumn("SOURCE_VOCABULARY", F.when(
            F.col("SOURCE_CODE").isNull(), F.lit(None)).otherwise(F.col("SOURCE_VOCABULARY")))

        return source_df

    def generate_concept_id(self, amc_concept_meta_df, final_out_dir):
        old_amc_source_concept_dir = self.latest_data_dir(PRE_OMOP_DATA_DIR, SOURCE_CONCEPT_META_DIR)
        if old_amc_source_concept_dir is None:
            old_amc_source_concept = self.create_empty_amc_source_concept_df()
        else:
            old_amc_source_concept = self.read_parquet_to_df(old_amc_source_concept_dir)

            # If we are rewriting SOURCE_CONCEPT_META of the same version with new additions,
            # Take a backup of the current SOURCE_CONCEPT_META so that we can revive old SOURCE_CONCEPT_META
            # during job failures
            if old_amc_source_concept_dir == final_out_dir:
                backup_path = os.path.join(self.DATA_DIR, PRE_OMOP_DATA_DIR, f"{SOURCE_CONCEPT_META_DIR}_{datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S')}")
                self.df_to_parquet(old_amc_source_concept, backup_path)
                # Read again from the backup path to avoid FileNotFound error while writing to same location
                old_amc_source_concept = self.read_parquet_to_df(backup_path)



        old_amc_source_concept = old_amc_source_concept.select(
            "SOURCE_CONCEPT_ID", "SOURCE_TABLE", "AMC_ID", "PATIENT_COUNT", "RECORD_COUNT").distinct()
        old_amc_source_concept = old_amc_source_concept.withColumnRenamed("PATIENT_COUNT", "OLD_PATIENT_COUNT")
        old_amc_source_concept = old_amc_source_concept.withColumnRenamed("RECORD_COUNT", "OLD_RECORD_COUNT")

        amc_concept_meta_df = amc_concept_meta_df.join(old_amc_source_concept, ["SOURCE_TABLE", "AMC_ID"], "left")
        max_source_concept_id = amc_concept_meta_df.agg(F.max("SOURCE_CONCEPT_ID")).collect()[0][0]
        if not max_source_concept_id:
            max_source_concept_id = 4000000000

        amc_concept_meta_df = amc_concept_meta_df.na.fill(0, subset=["SOURCE_CONCEPT_ID"])
        if self.RUN_MODE == RunMode.Delta.value:
            # When run mode is delta data records is read only from the incremental data
            # Update the counts of PATIENT_COUNT and RECORD_COUNTS for those concept which already exists
            amc_concept_meta_df = amc_concept_meta_df.na.fill(
                0, subset=["OLD_PATIENT_COUNT", "OLD_RECORD_COUNT", "PATIENT_COUNT", "RECORD_COUNT"])

            # The counts generated after won't be accurate if there are too many updates in the incremental data
            # Assumption is that updates won't be large in number to cause issues to counts and will get corrected in even runs
            amc_concept_meta_df = amc_concept_meta_df.withColumn(
                "PATIENT_COUNT", (F.col("OLD_PATIENT_COUNT") + F.col("PATIENT_COUNT")))
            amc_concept_meta_df = amc_concept_meta_df.withColumn(
                "RECORD_COUNT", (F.col("OLD_RECORD_COUNT") + F.col("RECORD_COUNT")))

        order_window = Window.partitionBy().orderBy("SOURCE_CONCEPT_ID", "SOURCE_TABLE", "AMC_ID")
        amc_concept_meta_df = amc_concept_meta_df.withColumn("SOURCE_CONCEPT_ID",
                                                             F.when(F.col("SOURCE_CONCEPT_ID").isin(0), F.lit(
                                                                 max_source_concept_id) + F.dense_rank().over(order_window))
                                                             .otherwise(F.col("SOURCE_CONCEPT_ID")))
        return amc_concept_meta_df

    def remove_duplicates_and_explode_codes(self, amc_source_concept_meta):

        # Duplicates occur because of same set of data going for multiple transactions
        # DIM generated from Conditions for Duke is added two times to
        # add two vocabularies icd9 and icd10, but when the codes are empty,
        # we get two rows with null values for icd9 and icd10, but with descirptions
        # AMC_ID| SOURCE_DESCRIPTION| SOURCE_CODE | SOURCE_VOCABULARY | ..
        # 1     | cancer            | Null        | Null
        # 1     | cancer            | Null        | Null
        # 3     | fever             | 234.5       | ICD9
        # 3     | fever             | abcd        | ICD10
        # We don't need two enties for cancer here

        # To remove duplicates, can't do distinct
        # because META columns are Struct and they don't support distinct
        # Hence do a group by on all the columns except META

        groupby_columns = amc_source_concept_meta.columns
        groupby_columns.remove("META")

        window = Window.partitionBy(groupby_columns).orderBy("AMC_ID")
        amc_source_concept_meta = amc_source_concept_meta.withColumn(
            "row", F.row_number().over(window)).filter(F.col("row") == 1).drop("row")

        # Split comma separated codes and explode (done for Duke)
        amc_source_concept_meta = amc_source_concept_meta.withColumn(
            "SOURCE_CODE", F.split("SOURCE_CODE", ', ').alias("SOURCE_CODE"))
        amc_source_concept_meta = amc_source_concept_meta.withColumn("SOURCE_CODE", F.explode_outer("SOURCE_CODE"))
        amc_source_concept_meta = amc_source_concept_meta.withColumn("SOURCE_CODE", F.trim(F.col("SOURCE_CODE")))
        return amc_source_concept_meta

    def run(self):

        table_list = self.parse_options_source()
        if self.options.run == "TEST":
            if not self.options.source_concept_write_path:
                raise Exception("Please provide a path to write source concept meta as argument --source_concept_write_path")
            if not self.options.dim_syn_read_path:
                print("*" * 100)
                print("WARNING!!! dim_syn_read_path is not specified, data will be read from orchestrator output paths")
                print("*" * 100)

            final_out_dir = self.options.source_concept_write_path
        elif self.options.run == "ALL":
            final_out_dir = os.path.join(self.DATA_DIR, PRE_OMOP_DATA_DIR, SOURCE_CONCEPT_META_DIR)
        else:
            raise Exception("Please add a run mode, accepted values are ALL, TEST")


        # Have an empty df with correct schema and use for joins later.
        amc_concept_meta_df = self.create_empty_amc_source_concept_df().drop("SOURCE_CONCEPT_ID")
        for table in table_list:
            source_df = self.generate_source_concept_meta(table)
            amc_concept_meta_df = amc_concept_meta_df.unionByName(source_df)
        amc_concept_meta_df = self.remove_duplicates_and_explode_codes(amc_concept_meta_df)
        amc_concept_meta_df = self.generate_concept_id(amc_concept_meta_df, final_out_dir)
        final_columns = self.create_empty_amc_source_concept_df().columns
        amc_concept_meta_df = amc_concept_meta_df.select(final_columns)
        self.df_to_parquet(amc_concept_meta_df, final_out_dir)


if __name__ == "__main__":
    SourceConceptGenerator().run()
