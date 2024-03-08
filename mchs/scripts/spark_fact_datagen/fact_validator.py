import glob
import os
import time
from pyspark.sql import *
from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType
from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator

VALIDATION_STATS_CONFIG  = 'GROUPBYColumnQueryTable.json'

class FactValidatorJob(Orchestrator):
    def __init__(self):
        super().__init__()
        self.parent = self

    def run(self):
        table_list = self.parse_options_source()
        if self.options.source == "ALL":
            table_list.insert(0, self.DIM_PATIENT.upper())
        # ORCH DATA VALIDATION TASKS
        if 'raw_col_counts' in self.options.run:
            self.gen_raw_column_counts(table_list)
        if 'data_col_counts' in self.options.run:
            self.gen_datagen_column_counts(table_list)
        if 'csv_col_counts' in self.options.run:
            self.validate_csv_column_counts(table_list)
        if 'version_counts' in self.options.run:
            self.validate_version_counts(table_list)
        if 'fact_lab_tests_convertor' in self.options.run:
            self.convert_fact_lab_tests()
        if 'count_comparator' in self.options.run:
            self.convert_parquet_to_csv(table_list)

    def gen_raw_column_counts(self, source_list):
        if self.ees_full_update():
            print("Skipping RAW_COL_COUNTS for EES-FULL-UPDATE")
            return

        begin = time.perf_counter()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        self.init_spark_session()
        dim_maps_df = self.read_versioned_dim_maps([self.PATIENT_DK, "NFER_PID"], version=run_version)
        dim_maps_df.cache()
        validation_config_file = os.path.join(os.path.dirname(self.config_file), VALIDATION_STATS_CONFIG)
        validation_config = self.get_json_data(validation_config_file)

        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            dtm_field = self.SYSTEM_DICT['data_tables'][source.lower()].get('dtm')
            column_list = validation_config.get(source, {}).get("RAW", [])
            if not column_list:
                print("No RAW columns found for", source)
                continue

            raw_source_df = self.raw_data_reader.read_raw_data(source)
            if not raw_source_df:
                print("No RAW DATA found for", source)
                continue
            if dtm_field and "DTM_YEAR" in column_list:
                raw_source_df = raw_source_df.withColumn("DTM_YEAR", F.split(dtm_field, '-')[0].cast(IntegerType()))

            column_list = [column for column in column_list if column in raw_source_df.columns]
            raw_source_df = raw_source_df.select(column_list + [self.PATIENT_DK])
            raw_source_df = raw_source_df.join(F.broadcast(dim_maps_df), self.PATIENT_DK, "left")
            if "NFER_PID" not in column_list:
                column_list.append("NFER_PID")
            raw_source_df = raw_source_df.select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in column_list])
            raw_source_df = raw_source_df.na.fill(0, ["NFER_PID"]).na.fill("EMPTY", column_list)
            raw_source_df.cache()

            for column in column_list:
                output_file = os.path.join(self.WRITE_ROOT_DIR, run_version, "VALIDATION", "COLUMN_COUNTS", source, "RAW_DATA", f"{column.lower()}.json")
                if self.skip_if_exists and self.glob(output_file) or column not in raw_source_df.columns:
                    continue
                column_stats = self.df_col_counts(raw_source_df, column)
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                self.dump_json_data(output_file, column_stats)
                print(f"Generated {output_file}")

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
            raw_source_df.unpersist()

        end = time.perf_counter()
        print("RAW_COL_COUNTS: Total Time =", end - begin)


    def gen_datagen_column_counts(self, source_list):
        self.init_spark_session()
        begin = time.perf_counter()
        validation_config_file = os.path.join(os.path.dirname(self.config_file), VALIDATION_STATS_CONFIG)
        print(f"Reading the config file {os.path.abspath(validation_config_file)}")
        validation_config = self.get_json_data(validation_config_file)
        print(f"The following sources would be validated {source_list}")
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        print(f"Running column counts for {run_version}")

        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            dtm_field = self.SYSTEM_DICT['data_tables'][source.lower()].get('dtm')

            column_list = validation_config.get(source, {}).get("PROCESSED", [])
            if not column_list:
                print(f"No detail found in the config file for {source}")
                continue

            run_version = self.options.version if self.options.version else self.RUN_VERSION
            source_dir = self.data_dir("DATAGEN", source, run_version)
            if not source_dir:
                print(f"Skipping {source} as the directory {source_dir} does not exist")
                continue

            source_df = self.read_versioned_datagen_dir(source, version=run_version)
            if dtm_field and "DTM_YEAR" in column_list:
                source_df = source_df.withColumn("DTM_YEAR", F.year(F.from_unixtime(dtm_field)))

            column_list = [column for column in column_list if column in source_df.columns]
            source_df = source_df.na.fill("EMPTY", column_list)
            source_df = source_df.select(["NFER_PID", "VERSION"] + column_list)
            source_df.cache()

            delta_df = source_df.filter(F.col("VERSION").isin(float(run_version)))
            delta_df.cache()

            print("Column List:", column_list)
            for column in column_list:
                if column not in source_df.columns:
                    continue

                output_file = os.path.join(self.WRITE_ROOT_DIR, run_version, "VALIDATION", "COLUMN_COUNTS", source, "DATAGEN", f"{column}.json")
                if not self.skip_if_exists or not self.glob(output_file):
                    column_stats = self.df_col_counts(source_df, column)
                    self.dump_json_data(output_file, column_stats)
                    print(f"Generated {output_file}")

                if self.RUN_MODE == RunMode.Delta.value:
                    output_file = os.path.join(self.WRITE_ROOT_DIR, run_version, "VALIDATION", "COLUMN_COUNTS", source, "DATAGEN_DELTA", f"{column}.json")
                    if not self.skip_if_exists or not self.glob(output_file):
                        column_stats = self.df_col_counts(delta_df, column)
                        self.dump_json_data(output_file, column_stats)
                        print(f"Generated {output_file}")

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
            self.SPARK_SESSION.catalog.clearCache()

        end = time.perf_counter()
        print("DATA_COL_COUNTS: Total Time =", end - begin)


    def df_col_counts(self, source_df: DataFrame, column):
        column_stats = {}
        row_count_df = source_df.select(column)
        row_count_df = row_count_df.withColumn("COUNT", F.lit(1))

        row_count_df = row_count_df.groupBy(column).agg(F.sum("COUNT"))
        for row in row_count_df.collect():
            column_stats.setdefault(row[0], row[1])

        pid_count_df = source_df.select(column, "NFER_PID")
        pid_count_df = pid_count_df.groupBy(column).agg(F.countDistinct("NFER_PID"))
        for row in pid_count_df.collect():
            column_stats[row[0]] = "{0}|{1}".format(row[1], column_stats[row[0]])

        return column_stats


    def validate_csv_column_counts(self, source_list):
        begin = time.perf_counter()
        failures = set()
        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            spark: SparkSession = self.init_spark_session(source)
            run_version = self.options.version if self.options.version else self.RUN_VERSION

            datagen_dir = os.path.join(self.ROOT_DIR, run_version, "DATAGEN", source)
            if not self.glob(datagen_dir):
                datagen_dir = os.path.join(self.LOCAL_ROOT_DIR, run_version, "DATAGEN", source)
                if not self.glob(datagen_dir):
                    continue

            headers = open(os.path.join(datagen_dir, "header.csv")).readline().split(self.csv_separator)
            header_count = len(headers)
            file_list = glob.glob(os.path.join(datagen_dir, "patient_*.final"))
            print(file_list[:3])
            non_empty_file_list = []
            for file in file_list:
                with open(file, encoding="utf8") as f:
                    for line in f:
                        if line != "\n":
                            non_empty_file_list.append(file)
                            break
            data_dir_path = os.path.join(datagen_dir, "patient_*.final")
            source_rdd = spark.sparkContext.textFile(",".join(non_empty_file_list))
            sep = self.csv_separator
            source_rdd = source_rdd.map(lambda x: x.split(sep))
            source_rdd = source_rdd.map(lambda x: ("{}|{}".format(x[0], x[1]), len(x)))
            source_rdd = source_rdd.reduceByKey(lambda x, y: x + y)
            source_rdd = source_rdd.filter(lambda x: x[1] != header_count)
            source_rdd = source_rdd.cache()
            err_count = source_rdd.count()
            if err_count > 0:
                print(f'{source} has {err_count} rows with incorrect column count')
                source_rdd.toDF(["pid_rowid", "count"]).show(10, False)
                failures.add(source)

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
            spark.catalog.clearCache()

        end = time.perf_counter()
        print("COLUMN_COUNTS: Total Time =", end - begin)
        if any(failures):
            raise Exception('VALIDATION FAILED:', list(failures))


    def validate_version_counts(self, source_list):
        begin = time.perf_counter()
        failures = set()
        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            spark: SparkSession = self.init_spark_session(source)
            run_version = self.options.version if self.options.version else self.RUN_VERSION

            datagen_dir = self.data_dir("DATAGEN", source)
            if not datagen_dir:
                continue

            fact_maps_dir = self.data_dir("FACT_MAPS_VERSIONED", source,run_version=run_version)
            if source.upper() == self.DIM_PATIENT.upper():
                fact_maps_dir = self.data_dir("DATAGEN", "DIM_MAPS_VERSIONED",run_version=run_version)
                continue

            if not fact_maps_dir:
                raise Exception(f'FACT_MAPS_VERSIONED does not exist for {source}')

            datagen_df = None
            for version in self.VERSION_LIST:
                if float(version) > float(run_version):
                    continue
                v_datagen_dir = self.data_dir("DATAGEN", source, run_version=version)
                if not v_datagen_dir:
                    continue
                v_datagen_df = self.read_final_dir(v_datagen_dir)
                v_datagen_df = self.assign_file_id_for_nfer_pid(v_datagen_df, source)
                v_datagen_df = v_datagen_df.select("FILE_ID", "NFER_PID", "ROW_ID")
                datagen_df = v_datagen_df if datagen_df is None else datagen_df.union(v_datagen_df)

            datagen_df = datagen_df.select("FILE_ID", "NFER_PID", "ROW_ID")
            datagen_df = datagen_df.groupBy("FILE_ID").agg(
                F.max("ROW_ID").alias("DG_MAX_ROW_ID"),
                F.count("ROW_ID").alias("DG_ROW_COUNT"),
                F.countDistinct("NFER_PID").alias("DG_PID_COUNT")
            )

            fact_maps_df = self.read_final_dir(fact_maps_dir)
            fact_maps_df = fact_maps_df.cache()

            is_guid_validated = True
            # filter(F.col("NFER_PID") != 0) for ignoring FACT_ONLY_PATIENTS that came in earlier versions
            if 'FACT_GUID' in fact_maps_df.columns:
                print(f"Validating all guids are enabled")
                c1 = fact_maps_df.filter(fact_maps_df.UPDATED_BY == '0.0').filter(F.col("NFER_PID") != 0).select('FACT_GUID').distinct().count()
                c2 = fact_maps_df.filter(F.col("NFER_PID") != 0).select('FACT_GUID').distinct().count()
                # there should not be two latest records with same guid
                c3 = fact_maps_df.filter(fact_maps_df.UPDATED_BY == '0.0').filter(F.col("NFER_PID") != 0).select('FACT_GUID').count()
                print(f"Count of guids for {source} latest = {c3}")
                print(f"Count of distinct guids for {source} latest = {c1} all = {c2}")
                is_guid_validated = (c1 == c2) and (c2 == c3)

            else:
                print(f'FACT_GUID does not exist for source {source}')

            if "FILE_ID" not in fact_maps_df.columns:
                fact_maps_df = self.assign_file_id_for_nfer_pid(fact_maps_df, source)

            count_maps_df = fact_maps_df.select("FILE_ID", "NFER_PID", "ROW_ID")
            count_maps_df = count_maps_df.groupBy("FILE_ID").agg(
                F.max("ROW_ID").alias("FP_MAX_ROW_ID"),
                F.count("ROW_ID").alias("FP_ROW_COUNT"),
                F.countDistinct("NFER_PID").alias("FP_PID_COUNT")
            )

            source_df = datagen_df.join(count_maps_df, "FILE_ID", "outer").na.fill(0)
            source_df = source_df.withColumn("ROW_COUNT_MISMATCH",
                F.when(
                    (F.col("DG_ROW_COUNT") != F.col("DG_MAX_ROW_ID")) |
                    (F.col("DG_ROW_COUNT") != F.col("FP_ROW_COUNT")) |
                    (F.col("FP_ROW_COUNT") != F.col("FP_MAX_ROW_ID"))
                    , 1
                ).otherwise(0))

            source_df = source_df.withColumn("PID_COUNT_MISMATCH",
                F.when(F.col("DG_PID_COUNT") != F.col("FP_PID_COUNT"), 1).otherwise(0))

            source_df = source_df.filter(F.col("ROW_COUNT_MISMATCH").isin(1) | F.col("PID_COUNT_MISMATCH").isin(1))
            source_df = source_df.cache()

            err_count = source_df.count()
            if err_count > 0 :
                row_count_mismatch = source_df.filter(F.col("ROW_COUNT_MISMATCH").isin(1)).count()
                pid_count_mismatch = source_df.filter(F.col("PID_COUNT_MISMATCH").isin(1)).count()
                print(f'{source} has {err_count} files with mismatched version counts')
                print(f'{source} has {row_count_mismatch} files with mismatched row counts')
                print(f'{source} has {pid_count_mismatch} files with mismatched pid counts')
                source_df.show(5, False)
                failures.add(source)

            if not is_guid_validated:
                print(f"Guid validation failed")
                if source not in failures:
                    failures.add(source)

            if err_count==0 and is_guid_validated:
                print(f'VERSION COUNT VALIDATION PASSED FOR {source}')

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
            spark.catalog.clearCache()

        end = time.perf_counter()
        print("VERSION_COUNTS: Total Time =", end - begin)
        if any(failures):
            raise Exception('VALIDATION FAILED:', list(failures))
        print("DTM_FIELDS VALIDATION: Total Time =", end - begin)

    def convert_fact_lab_tests(self):
        source = "FACT_LAB_TEST"
        self.init_spark_session()
        latest_data_dir = os.path.join(self.ROOT_DIR, "5.008", "DELTA_TABLES","FACT_TABLES", f"{source}.parquet" )
        source_df = self.read_delta_to_df(latest_data_dir)
        final_schema = self.read_final_schema(source)
        #for version in self.VERSION_LIST:
        for version in ["5.0"]:
            # if float(version) != 5.000:
            #     continue
            version_df = source_df.filter(F.col("VERSION").isin(version)).select(final_schema)
            print(f"{version_df.count()}")
            self.df_to_parquet(version_df, os.path.join(self.WRITE_ROOT_DIR, version, "DATAGEN.parquet", source))

    def convert_parquet_to_csv(self, source_list):
        self.init_spark_session()
        for source in source_list:
                for version in self.VERSION_LIST:
                    print(f"Version is {version}")
                    if float(version) < 5.008:
                        latest_data_dir_generated = os.path.join(self.ROOT_DIR, version, "DATAGEN.parquet",source )
                        latest_data_dir_original = os.path.join(self.ROOT_DIR, version, "DATAGEN", source)
                        source_df_generated = self.read_parquet_to_df(latest_data_dir_generated)
                        source_df_original = self.read_final_dir(latest_data_dir_original)
                        print(f"1xCount Mismatch for {source}: for {version} \n Counts for Original {source_df_original.count()}, \n "
                                    f"Generated{source_df_generated.count()}")
                        if source_df_generated.count() == source_df_original.count():
                            self.df_to_patient_blocks(source_df_generated,
                                                     os.path.join(self.LOCAL_ROOT_DIR, version,"DATAGEN", source),
                                                     source_df_generated.columns)
                        else:
                            print(f"2xCount Mismatch for {source}: for {version} \n Counts for Original {source_df_original.count()}, \n "
                                                                    f"Generated{source_df_generated.count()}")
                            continue

                    # self.write_df_to_csv(version_df, os.path.join(self.ROOT_DIR, version, "DATAGEN.parquet", source))


if __name__ == '__main__':
    FactValidatorJob().run()