import os
import time
from collections import Counter

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from core.config_vars import RunMode

from spark_jobs.orchestrator import Orchestrator


class FactVersionJob(Orchestrator):

    def __init__(self):
        super().__init__()
        self.valid = True


    def run(self):
        table_list = self.parse_options_source()
        if 'fact_version' in self.options.run:
            self.version_fact_data(table_list)
        if 'validate' in self.options.run:
            self.validate_fact_maps(table_list)
        if 'version_dir_map' in self.options.run:
            self.map_version_directories()
        if 'max_row_id' in self.options.run:
            self.gen_max_row_id(table_list)
        if 'parquet2final' in self.options.run:
            self.parquet_to_final(table_list)
        if 'generate_stats' in self.options.run:
            self.generate_stats_only(table_list)

    def reincarnate_dead_guids(self, fact_maps_df):
        latest_fact_guids = fact_maps_df.filter(fact_maps_df.MAP_UPDATED_BY == '0.0').select('MAP_FACT_GUID').distinct()
        all_fact_guids = fact_maps_df.select('MAP_FACT_GUID').distinct()
        latest_count = latest_fact_guids.count()
        all_count = all_fact_guids.count()
        print(f"all_fact_guids_count: {all_count} latest_count: {latest_count}")

        if latest_count != all_count:
            print("Fixing fact guids")
            disabled_guid_df = all_fact_guids.join(latest_fact_guids, on='MAP_FACT_GUID', how='left_anti')

            max_updated_by_df = fact_maps_df.select(['MAP_FACT_GUID', 'MAP_UPDATED_BY']).join(disabled_guid_df, on='MAP_FACT_GUID')\
                                            .groupBy('MAP_FACT_GUID').agg(
                                                F.max("MAP_UPDATED_BY").alias("max_updated_by"),
                                            ).select(['MAP_FACT_GUID', 'max_updated_by'])

            fact_maps_df = fact_maps_df.join(max_updated_by_df, 'MAP_FACT_GUID', 'left')
            fact_maps_df = fact_maps_df.withColumn("MAP_UPDATED_BY",
                                                   F.when(F.col("MAP_UPDATED_BY") == F.col("max_updated_by"),
                                                          0.0).otherwise(F.col("MAP_UPDATED_BY"))).drop("max_updated_by")
        return fact_maps_df

    def validate_guid_change(self, df,source):
        c1 = df.filter(df.UPDATED_BY == '0.0').select('FACT_GUID').distinct().count()
        c2 = df.select('FACT_GUID').distinct().count()
        print(f"Count of guids for {source} latest = {c1} all = {c2}")
        return c1 == c2

    def generate_stats_only(self,source_list):
        begin = time.perf_counter()

        for source in source_list:
            start = time.perf_counter()
            print(f"Generating stats for {source}")
            stats_file = os.path.join(self.DATA_DIR, "FACT_MAPS_VERSIONED/STATS", f"{source.lower()}.json")
            if self.skip_if_exists and self.glob(stats_file):
                print(f"Source {source} already Generated.")
                continue
            fact_maps_dir = self.data_dir("FACT_MAPS_VERSIONED", source)
            if not fact_maps_dir or not self.glob(fact_maps_dir):
                raise Exception(f"ERROR: No current FACT_MAPS_VERSIONED for {source} found.. ")

            self.init_spark_session(source)
            fact_maps_df = self.read_final_dir(fact_maps_dir)
            counter = Counter()
            counter.update({"fact_rows_all_versions": fact_maps_df.count()})
            counter.update(
                {"fact_rows_curr_version": fact_maps_df.filter(F.col("VERSION").isin(float(self.RUN_VERSION))).count()})
            counter.update(
                {"fact_row_updates": fact_maps_df.filter(F.col("UPDATED_BY").isin(float(self.RUN_VERSION))).count()})
            counter.update({"fact_row_active": fact_maps_df.filter(F.col("UPDATED_BY").isin(0.0)).count()})

            self.log_spark_stats("FACT_MAPS_VERSIONED", source, counter)

            self.SPARK_SESSION.catalog.clearCache()
            print(f"time for {source}: {(time.perf_counter() - start) / 60} min\n")

        print("FACT VERSIONED: Total Time =", time.perf_counter() - begin)

    def version_fact_data(self, source_list):
        version = self.options.version if self.options.version else self.RUN_VERSION
        begin = time.perf_counter()
        failures = []
        for source in source_list:
            start = time.perf_counter()
            print(f"Versioning {source}")
            self.init_spark_session(source)

            if not self.data_dir("DATAGEN", source,run_version=version):
                print(f"Source {source} does not exist in DATAGEN")
                continue

            if self.skip_if_exists and self.data_dir("FACT_MAPS_VERSIONED", source,run_version=version):
                continue

            delta_maps_df = self.load_delta_maps_df(source,version=version)
            old_fact_maps_df = self.load_fact_maps_df(source,version=version)

            if source not in ['FACT_FLOWSHEETS']:
                old_fact_maps_df.cache()

            # Patch for inactive fact_guids applied in 5.015
            if False:#self.RUN_VERSION == "5.015":
                print("Applying reincarnate patch ")
                old_fact_maps_df = self.reincarnate_dead_guids(old_fact_maps_df)

            final_fact_maps_df = self.stamp_version_and_updates(source, delta_maps_df, old_fact_maps_df,version=version)

            fact_maps_schema, _ = self.read_fact_map_schema()
            self.write_final_dir("FACT_MAPS_VERSIONED", final_fact_maps_df, fact_maps_schema, source,version=version)
            self.SPARK_SESSION.catalog.clearCache()
            print(f"time for {source}: {(time.perf_counter()-start)/60} min\n")

        end = time.perf_counter()
        print("FACT VERSIONED: Total Time =", end - begin)
        if failures:
            raise Exception("FACT VERSIONED: Failures =", failures)

    def validate_fact_maps(self, source_list):
        begin = time.perf_counter()
        failures = []
        for source in source_list:
            start = time.perf_counter()
            fact_maps_dir = self.data_dir("FACT_MAPS_VERSIONED", source)
            if not fact_maps_dir:
                print(f"Source {source} does not exist in FACT_MAPS_VERSIONED")
                continue

            stats_file = os.path.join(self.DATA_DIR, "FACT_MAPS_VERSIONED/STATS", f"{source.lower()}.json")
            if self.skip_if_exists and self.glob(stats_file):
                print(f"Source {source} already validated")
                continue

            self.init_spark_session(source)
            delta_maps_count = self.load_delta_maps_df(source).count()
            old_fact_maps_count = self.load_fact_maps_df(source).count()

            final_fact_maps_df = self.read_final_dir(fact_maps_dir)
            final_fact_maps_df = self.unpack_fact_guid(final_fact_maps_df, columns=["PDK_ID"])
            final_fact_maps_df = final_fact_maps_df.repartition("PDK_ID").cache()

            final_fact_maps_count = final_fact_maps_df.count()
            counts = final_fact_maps_count, old_fact_maps_count, delta_maps_count
            self.valid = True

            # Validating across versions
            if self.RUN_MODE == RunMode.Delta.value:
                self.validate_unique_fact_ids(source, final_fact_maps_df)

            # Validating only active maps
            active_maps_df = final_fact_maps_df.filter(F.col("UPDATED_BY").isin(0.0))
            self.validate_fact_version(source, active_maps_df)
            self.validate_empty_rows(source, active_maps_df)
            self.validate_old_pid_version(source, active_maps_df)
            self.validate_fact_counts(source, active_maps_df, counts)

            print("FACT MAPS VALIDATION {1}: {0}".format(source, "PASSED" if self.valid else "FAILED"))
            fact_maps_dir = fact_maps_dir.replace(self.ROOT_DIR, self.WRITE_ROOT_DIR)
            if self.valid:
                self.generate_version_counts(final_fact_maps_df, fact_maps_dir)
                self.log_version_stats(source, final_fact_maps_df, final_fact_maps_count, start)
            else:
                failures.append(source)
                failure_maps_dir = fact_maps_dir.replace("FACT_MAPS_VERSIONED", "FACT_MAPS_VERSIONED_FAILURES")
                self.rename_file(fact_maps_dir,failure_maps_dir)

            self.SPARK_SESSION.catalog.clearCache()

        end = time.perf_counter()
        print("FACT MAPS VALIDATION: Total Time =", end - begin)
        if failures:
            raise Exception("FACT MAPS VALIDATION: Failures =", failures)

    def stamp_version_and_updates(self, source, source_maps_df: DataFrame, fact_maps_df: DataFrame,version=None):
        # MERGE FACT_MAPS + SOURCE_MAPS
        source_fact_maps_df = self.merge_all_maps(source, fact_maps_df, source_maps_df,version=version)
        source_fact_maps_df.cache()
        fact_maps_df.unpersist()

        # PROCESSING NEW ROWS
        delta_fact_maps_df = source_fact_maps_df.filter(
            F.col("DATA_NFER_PID").isNotNull() &
            (F.col("MAP_UPDATED_BY").isin(0.0) | F.col("MAP_UPDATED_BY").isNull())
        )
        delta_fact_maps_df = self.generate_fact_id(delta_fact_maps_df)
        delta_fact_maps_df = self.generate_fact_guid(delta_fact_maps_df)

        delta_fact_maps_df = self.assign_version_and_schema(delta_fact_maps_df,version)
        delta_fact_maps_df = self.assign_file_id_for_nfer_pid(delta_fact_maps_df, source)

        if self.RUN_MODE == RunMode.Delta.value and not self.ees_full_update():
            delta_fact_maps_df = self.increment_row_id(source, delta_fact_maps_df,version=version)

        # MERGE DELTA + FULL MAPS
        source_fact_maps_df = self.mark_updated_by_version(source_fact_maps_df,version=version)

        final_fact_maps_df = self.union_delta_with_full_maps(delta_fact_maps_df, source_fact_maps_df)
        final_fact_maps_df = self.assign_file_id_for_nfer_pid(final_fact_maps_df, source)

        return final_fact_maps_df


    def check_output_dir(self, source):
        fact_maps_dir = os.path.join(self.DATA_DIR, "FACT_MAPS_VERSIONED", source)
        stats_file = os.path.join(self.DATA_DIR, "FACT_MAPS_VERSIONED/STATS", f"{source.lower()}.json")
        if self.skip_if_exists and self.glob(os.path.join(fact_maps_dir, "header.csv")):
            print(f'{source} already exists. Skipping generation.')
            return None

        if not self.glob(os.path.join(self.DATAGEN_DIR, source)):
            print(f"Source {source} does not exist in DATAGEN")
            return None

        return fact_maps_dir


    def read_source_maps_df(self, source,version=None):
        if not version:
            version = self.RUN_VERSION
        unique_schema = self.read_unique_schema(source)
        timestamp_fields = self.get_timestamp_fields(source)
        read_schema = list(set(["ROW_ID", self.PATIENT_DK] + [key.split('.')[0] for key in unique_schema]))

        source_maps_df = self.read_source_files(source, version)
        source_maps_df = source_maps_df.select(read_schema)

        dim_maps_df = self.read_versioned_dim_maps([self.PATIENT_DK, "NFER_PID", "PDK_ID"], version=version)
        source_maps_df = source_maps_df.join(F.broadcast(dim_maps_df), [self.PATIENT_DK], "left")
        source_maps_df = source_maps_df.na.fill(0, subset=["NFER_PID", "PDK_ID"])

        if self.RELATIVE_DTM:
            schema = ["NFER_PID", self.ENCOUNTER_EPOCH, "PATIENT_MERGED_FLAG"]
            patient_maps_df = self.read_versioned_datagen_dir(self.DIM_PATIENT, columns=schema,version=version)
            patient_maps_df = self.de_dupe_patient_meta(patient_maps_df)
            source_maps_df = source_maps_df.join(F.broadcast(patient_maps_df), ["NFER_PID"], "left")

        for field in unique_schema:
            if field in timestamp_fields:
                source_maps_df = self.parse_timestamp(source_maps_df, field)

        source_maps_df = self.calc_key_hash(source_maps_df, unique_schema)
        source_maps_df = source_maps_df.select("ROW_ID", "KEY_HASH", "NFER_PID", "PDK_ID")
        return source_maps_df


    def read_datagen_maps_df(self, source,version=None):
        if not version:
            version = self.RUN_VERSION
        datagen_maps_dir = os.path.join(self.ROOT_DIR, version, "DATAGEN_MAPS", source)
        datagen_maps_df = self.read_parquet_to_df(datagen_maps_dir)
        datagen_maps_size = self.get_dir_size_in_gb(datagen_maps_dir)

        if "KEY_HASH" not in datagen_maps_df.columns:
            fact_maps_dir = self.last_data_dir("FACT_MAPS_VERSIONED", source,run_version=version)
            fact_maps_df = self.read_final_dir(fact_maps_dir)
            fact_maps_df = fact_maps_df.filter(F.col("UPDATED_BY").isin(0.0))

            fact_map_schema = ["KEY_HASH", "FACT_GUID"]
            if "PDK_ID" not in datagen_maps_df.columns:
                fact_maps_df = self.unpack_fact_guid(fact_maps_df)
                fact_map_schema += ["PDK_ID"]
            fact_maps_df = fact_maps_df.select(fact_map_schema)

            join_df = datagen_maps_df if datagen_maps_size > 1 else F.broadcast(datagen_maps_df)
            datagen_maps_df = fact_maps_df.join(join_df, ["FACT_GUID"]).drop("FACT_GUID")

        return datagen_maps_df


    def load_delta_maps_df(self, source,version=None):
        if self.ees_full_update():
            source_maps_df = self.read_datagen_maps_df(source,version)
        else:
            source_maps_df = self.read_source_maps_df(source,version)
        fact_table_id = self.SYSTEM_DICT['data_tables'][source.lower()]['fact_table_id']
        source_maps_df = source_maps_df.withColumn("FACT_TABLE_ID", F.lit(fact_table_id))
        for column in source_maps_df.columns:
            if column not in ['KEY_HASH']:
                source_maps_df = source_maps_df.withColumnRenamed(column, "DATA_" + column)

        return source_maps_df


    def load_fact_maps_df(self, source, version=None):
        if self.RUN_MODE == RunMode.Full.value and self.OLD_MAPS_DIR:
            fact_maps_dir = os.path.join(self.OLD_MAPS_DIR, "FACT_MAPS_VERSIONED", source)
        else:
            fact_maps_dir = self.last_data_dir("FACT_MAPS_VERSIONED", source,run_version=version)

        if not fact_maps_dir or not self.glob(fact_maps_dir):
            print(f"WARNING: No previous FACT_MAPS_VERSIONED for {source} found")
            fact_maps_df = self.create_empty_fact_maps_df()
        else:
            fact_maps_df = self.read_final_dir(fact_maps_dir)

        fact_maps_df = self.unpack_fact_guid(fact_maps_df)
        for column in fact_maps_df.columns:
            if column != 'KEY_HASH':
                fact_maps_df = fact_maps_df.withColumnRenamed(column, "MAP_" + column)

        return fact_maps_df


    def create_empty_fact_maps_df(self):
        nfer_schema, type_schema = self.read_fact_map_schema()
        fact_maps_df = self.SPARK_SESSION.createDataFrame([], self.build_schema(nfer_schema, type_schema))
        return fact_maps_df


    def load_obsolete_pids(self,run_version=None):

        if not run_version:
            run_version = self.RUN_VERSION
        nfer_schema = ["MAP_PDK_ID", "PID_CHANGE_VERSION"]
        type_schema = ["INTEGER", "STRING"]
        if self.RUN_MODE == RunMode.Full.value or self.ees_full_update():
            return self.SPARK_SESSION.createDataFrame([], self.build_schema(nfer_schema, type_schema))

        obsolete_pid_df = self.read_pid_change_maps(run_version=run_version)

        if not obsolete_pid_df:
            return self.SPARK_SESSION.createDataFrame([], self.build_schema(nfer_schema, type_schema))

        if 'OLD_PDK_ID' in obsolete_pid_df.columns:
            obsolete_pid_df = obsolete_pid_df.select('OLD_PDK_ID')
            obsolete_pid_df = obsolete_pid_df.withColumnRenamed("OLD_PDK_ID", "MAP_PDK_ID")
        elif 'OLD_NFER_PID' in obsolete_pid_df.columns:
            print("Loading obsolete PIDs the old way. Should not be done in newer versions")
            obsolete_pid_df = obsolete_pid_df.select('OLD_NFER_PID')
            obsolete_pid_df = obsolete_pid_df.withColumnRenamed("OLD_NFER_PID", "MAP_NFER_PID")

        obsolete_pid_df = obsolete_pid_df.distinct()
        obsolete_pid_df = obsolete_pid_df.withColumn("PID_CHANGE_VERSION", F.lit(run_version))
        return obsolete_pid_df

    def merge_all_maps(self, source, fact_maps_df: DataFrame, source_maps_df: DataFrame, version=None):
        # fact_maps with data from datagen_maps
        join_type = "LEFT" if self.RUN_MODE == RunMode.Full.value else "FULL"
        condition = ["KEY_HASH"]
        if source == "FACT_RHC_MEASUREMENTS":
            source_maps_df = source_maps_df.withColumn("JOIN_PDK_ID", F.col("DATA_PDK_ID"))
            fact_maps_df = fact_maps_df.withColumn("JOIN_PDK_ID", F.col("MAP_PDK_ID"))
            condition = ["KEY_HASH", "JOIN_PDK_ID"]
        source_fact_maps_df = source_maps_df.join(fact_maps_df, condition, join_type)
        source_maps_df = source_maps_df.drop("JOIN_PDK_ID")
        fact_maps_df = fact_maps_df.drop("JOIN_PDK_ID")

        obsolete_pid_df = self.load_obsolete_pids(run_version=version)
        if obsolete_pid_df:
            if 'MAP_PDK_ID' in obsolete_pid_df.columns:
                source_fact_maps_df = source_fact_maps_df.join(F.broadcast(obsolete_pid_df), ["MAP_PDK_ID"], "LEFT")
            elif 'MAP_NFER_PID' in obsolete_pid_df.columns:
                source_fact_maps_df = source_fact_maps_df.join(F.broadcast(obsolete_pid_df), ["MAP_NFER_PID"], "LEFT")

        max_factid_df, max_keyid_df = self.compute_max_ids(fact_maps_df)
        source_fact_maps_df = source_fact_maps_df.join(F.broadcast(max_factid_df), ["DATA_NFER_PID"], "LEFT")
        source_fact_maps_df = source_fact_maps_df.join(F.broadcast(max_keyid_df), ["DATA_PDK_ID"], "LEFT")

        source_fact_maps_df = source_fact_maps_df.na.fill(-1, ["MAP_FACT_ID", "MAP_FACT_KEY_ID"])
        source_fact_maps_df = source_fact_maps_df.na.fill(0, ["MAX_FACT_ID", "MAX_KEY_ID"])
        return source_fact_maps_df


    def compute_max_ids(self, fact_maps_df: DataFrame):
        max_factid_df = fact_maps_df.groupBy("MAP_NFER_PID").agg(F.max("MAP_FACT_ID").alias("MAX_FACT_ID"))
        max_factid_df = max_factid_df.withColumnRenamed("MAP_NFER_PID", "DATA_NFER_PID")

        max_keyid_df = fact_maps_df.groupBy("MAP_PDK_ID").agg(F.max("MAP_FACT_KEY_ID").alias("MAX_KEY_ID"))
        max_keyid_df = max_keyid_df.withColumnRenamed("MAP_PDK_ID", "DATA_PDK_ID")

        return max_factid_df, max_keyid_df


    def mark_updated_by_version(self, source_fact_maps_df: DataFrame,version=None):
        run_version = version if version else self.RUN_VERSION
        if self.RUN_MODE == RunMode.Full.value:
            source_fact_maps_df = source_fact_maps_df.withColumn("MAP_UPDATED_BY", F.lit(0.0))
            return source_fact_maps_df

        source_fact_maps_df = source_fact_maps_df.withColumn("MAP_UPDATED_BY",
            F.when(
                F.col("DATA_NFER_PID").isNotNull() &
                F.col("MAP_NFER_PID").isNotNull() &
                F.col("MAP_UPDATED_BY").isin(0.0),
                    F.lit(float(run_version)))
            .when(
                F.col("PID_CHANGE_VERSION").isNotNull() &
                F.col("MAP_UPDATED_BY").isin(0.0),
                    F.col("PID_CHANGE_VERSION"))
            .otherwise(F.col("MAP_UPDATED_BY")))
        return source_fact_maps_df


    def generate_fact_id(self, delta_fact_maps_df: DataFrame):
        # Group by NFER_PID and Order by MAP_FACT_ID, KEY_HASH
        # All unassigned MAP_FACT_ID have value as -1, so they would be top of the list
        pid_window = Window.partitionBy("DATA_NFER_PID").orderBy("MAP_FACT_ID", "KEY_HASH")

        # If NFER_PID changes across version for same KEY_HASH, then force MAP_FACT_ID as -1 to assign new FACT_ID
        delta_fact_maps_df = delta_fact_maps_df.withColumn("MAP_FACT_ID",
            F.when(F.col("MAP_NFER_PID") != F.col("DATA_NFER_PID"), F.lit(-1)).otherwise(F.col("MAP_FACT_ID")))

        # If MAP_FACT_ID is -1, then assign new FACT_ID
        delta_fact_maps_df = delta_fact_maps_df.withColumn("FACT_ID",
            F.when(F.col("MAP_FACT_ID").isin(-1), F.col("MAX_FACT_ID") + F.dense_rank().over(pid_window))
            .otherwise(F.col("MAP_FACT_ID")).cast("INT"))

        return delta_fact_maps_df


    def generate_fact_guid(self, delta_fact_maps_df: DataFrame):
        # Group by PDK_ID and Order by MAP_FACT_KEY_ID, KEY_HASH
        # All unassigned MAP_FACT_KEY_ID have value as -1, so they would be top of the list
        pdk_window = Window.partitionBy("DATA_PDK_ID").orderBy("MAP_FACT_KEY_ID", "KEY_HASH")

        # If MAP_FACT_KEY_ID is -1, then assign new FACT_KEY_ID
        delta_fact_maps_df = delta_fact_maps_df.withColumn("FACT_KEY_ID",
            F.when(F.col("MAP_FACT_KEY_ID").isin(-1), F.col("MAX_KEY_ID") + F.dense_rank().over(pdk_window))
            .otherwise(F.col("MAP_FACT_KEY_ID")).cast("INT"))

        # Generate FACT_GUID(64) = FACT_TABLE_ID(8) + PDK_ID(28) + FACT_KEY_ID(28)
        delta_fact_maps_df = self.pack_fact_guid(delta_fact_maps_df, fields=["DATA_FACT_TABLE_ID", "DATA_PDK_ID", "FACT_KEY_ID"])
        return delta_fact_maps_df


    def assign_version_and_schema(self, delta_fact_maps_df: DataFrame,version=None):
        run_version = version if version else self.RUN_VERSION
        data_columns = [column for column in delta_fact_maps_df.columns if not column.startswith("MAP_")]
        for column in data_columns:
            delta_fact_maps_df = delta_fact_maps_df.withColumnRenamed(column, column.replace("DATA_", ""))

        delta_fact_maps_df = delta_fact_maps_df.withColumn("VERSION", F.lit(float(run_version)))
        delta_fact_maps_df = delta_fact_maps_df.withColumn("UPDATED_BY", F.lit(0.0))

        fact_map_schema, _ = self.read_fact_map_schema()
        delta_fact_maps_df = delta_fact_maps_df.select(fact_map_schema)
        return delta_fact_maps_df


    def increment_row_id(self, source: str, delta_fact_maps_df: DataFrame,version=None):
        max_rowid_dir = self.data_dir("MAX_ROW_ID", source,run_version=version)
        if not max_rowid_dir:
            return delta_fact_maps_df
        max_rowid_df = self.read_parquet_to_df(max_rowid_dir)
        #left join to accomodate the changing row ids.
        delta_fact_maps_df = delta_fact_maps_df.join(F.broadcast(max_rowid_df), "FILE_ID",how="left").fillna(0,"MAX_ROW_ID")
        delta_fact_maps_df = delta_fact_maps_df.withColumn("ROW_ID", F.col("ROW_ID") + F.col("MAX_ROW_ID")).drop("MAX_ROW_ID")
        return delta_fact_maps_df


    def union_delta_with_full_maps(self, delta_fact_maps_df:DataFrame, source_fact_maps_df:DataFrame):
        fact_map_schema = self.read_fact_map_schema()[0]
        fact_maps_df = source_fact_maps_df.filter(F.col("MAP_NFER_PID").isNotNull())
        maps_columns = [column for column in source_fact_maps_df.columns if not column.startswith("DATA_")]
        for column in maps_columns:
            fact_maps_df = fact_maps_df.withColumnRenamed(column, column.replace("MAP_", ""))
        fact_maps_df = fact_maps_df.select(fact_map_schema)
        fact_maps_df = fact_maps_df.union(delta_fact_maps_df.select(fact_map_schema))
        return fact_maps_df


    def validate_empty_rows(self, source, fact_maps_df:DataFrame):
        empty_maps_counts = fact_maps_df.filter(F.col("KEY_HASH").isNull()).count()
        empty_fact_id = fact_maps_df.filter(F.col("FACT_ID").isNull()).count()
        empty_fact_guid = fact_maps_df.filter(F.col("FACT_GUID").isNull()).count()
        if empty_maps_counts > 0 or empty_fact_id > 0 or empty_fact_guid > 0:
            print(f'{source} has empty KEY_HASH or FACT_ID or FACT_GUID')
            print(f'empty_key_hash = {empty_maps_counts}, empty_fact_id = {empty_fact_id}, empty_fact_guid = {empty_fact_guid}')
            self.valid = False


    def validate_unique_fact_ids(self, source, fact_maps_df:DataFrame):
        validation_maps_df = fact_maps_df.withColumn("KEY_ID_HASH", F.concat_ws("|", F.array(["KEY_HASH", "FACT_GUID"])))

        window = Window.partitionBy("PDK_ID").orderBy("KEY_ID_HASH")
        unqiue_maps_df = validation_maps_df.withColumn("DUPLICATE", F.lag("KEY_ID_HASH", 1, "").over(window) == F.col("KEY_ID_HASH"))
        unqiue_maps_df = unqiue_maps_df.filter(F.col("DUPLICATE") == False).drop("DUPLICATE").drop("KEY_ID_HASH")
        unqiue_maps_df = unqiue_maps_df.groupBy("PDK_ID").agg(
            F.count("*").alias("UNIQUE_COUNT"),
            F.countDistinct("KEY_HASH").alias("UNIQUE_KEY_COUNT"),
            F.countDistinct("FACT_GUID").alias("UNIQUE_FACT_GUID_COUNT")
        ).cache()

        invalid_pdk_df = unqiue_maps_df.filter(F.col("UNIQUE_COUNT") != F.col("UNIQUE_KEY_COUNT")).cache()
        if invalid_pdk_df.count() > 0:
            print("ACROSS VERSION KEY_HASH VALIDATION FAILED for", source)
            invalid_maps_df = validation_maps_df.join(F.broadcast(invalid_pdk_df), on=["PDK_ID"])
            invalid_maps_df.groupBy("KEY_HASH").agg(F.count("*").alias("COUNT")).filter(F.col("COUNT") > 1).show(10, truncate=False)
            self.valid = False
        else:
            print("ACROSS VERSION KEY_HASH VALIDATION PASSED for", source)

        invalid_pdk_df = unqiue_maps_df.filter(F.col("UNIQUE_COUNT") != F.col("UNIQUE_FACT_GUID_COUNT")).cache()
        if invalid_pdk_df.count() > 0:
            print("ACROSS VERSION FACT_GUID VALIDATION FAILED for", source)
            invalid_maps_df = validation_maps_df.join(F.broadcast(invalid_pdk_df), on=["PDK_ID"])
            invalid_maps_df.groupBy("FACT_GUID").agg(F.count("*").alias("COUNT")).filter(F.col("COUNT") > 1).show(10, truncate=False)
            self.valid = False
        else:
            print("ACROSS VERSION FACT_GUID VALIDATION PASSED for", source)


    def validate_fact_version(self, source, fact_maps_df: DataFrame):
        validation_maps_df = fact_maps_df.withColumn("FACT_ID", F.concat(F.col("NFER_PID"), F.lit("#"), F.col("FACT_ID")))

        unqiue_maps_df = validation_maps_df.groupBy("PDK_ID").agg(
            F.count("*").alias("UNIQUE_COUNT"),
            F.countDistinct("KEY_HASH").alias("UNIQUE_KEY_COUNT"),
            F.countDistinct("FACT_ID").alias("UNIQUE_FACT_ID_COUNT")
        )

        invalid_pdk_df = unqiue_maps_df.filter(F.col("UNIQUE_COUNT") != F.col("UNIQUE_KEY_COUNT"))
        if invalid_pdk_df.count() > 0:
            print("VERSION SPECIFIC KEY_HASH VALIDATION FAILED for", source)
            invalid_maps_df = validation_maps_df.join(F.broadcast(invalid_pdk_df), on=["PDK_ID"])
            invalid_maps_df.groupBy("KEY_HASH").agg(F.count("*").alias("COUNT")).filter(F.col("COUNT") > 1).show(10, truncate=False)
            self.valid = False
        else:
            print("VERSION SPECIFIC KEY_HASH VALIDATION PASSED for", source)

        if source in self.SIGNAL_TABLES:
            invalid_pdk_df = unqiue_maps_df.filter(F.col("UNIQUE_COUNT") != F.col("UNIQUE_FACT_ID_COUNT"))
            if invalid_pdk_df.count() > 0:
                print("VERSION SPECIFIC FACT_ID VALIDATION FAILED for", source)
                invalid_maps_df = validation_maps_df.join(F.broadcast(invalid_pdk_df), on=["PDK_ID"])
                invalid_maps_df.groupBy("FACT_ID").agg(F.count("*").alias("COUNT")).filter(F.col("COUNT") > 1).show(10, truncate=False)
                self.valid = False
            else:
                print("VERSION SPECIFIC FACT_ID VALIDATION PASSED for", source)


    def validate_old_pid_version(self, source, fact_maps_df:DataFrame):
        pid_change_df = self.read_pid_change_maps()
        if not pid_change_df:
            return
        patient_map_dir = self.latest_data_dir("DATAGEN", "DIM_MAPS_VERSIONED")
        patient_map_df = self.read_final_dir(patient_map_dir)
        pid_change_df = pid_change_df.select("OLD_NFER_PID")
        pid_change_df = pid_change_df.withColumnRenamed("OLD_NFER_PID", "NFER_PID")
        patient_map_df = patient_map_df.filter(F.col("UPDATED_BY") == "0.0").select("NFER_PID")
        invalid_pid_df = patient_map_df.join(pid_change_df, ["NFER_PID"])
        sample_pids = invalid_pid_df.select(F.col("NFER_PID")).rdd.flatMap(lambda x: x).collect()
        pid_change_df = pid_change_df.filter(~F.col("NFER_PID").isin(sample_pids))
        pid_change_df = pid_change_df.withColumn("OLD_PID", F.lit("Y"))

        # KNOWN ISSUE WITH MAYO DATA, NEEDS TO BE FIXED
        old_active_pids = [93301405, 24003643]
        pid_change_df = pid_change_df.filter(~F.col("NFER_PID").isin(old_active_pids))

        fact_maps_df = fact_maps_df.join(F.broadcast(pid_change_df), on=["NFER_PID"], how="left")
        fact_maps_df = fact_maps_df.filter(F.col("OLD_PID").isin("Y"))

        invalid_fact_maps_df = fact_maps_df.filter(F.col("UPDATED_BY").isin(0.0)).cache()
        invalid_fact_maps_count = invalid_fact_maps_df.count()
        if invalid_fact_maps_count > 0:
            print(f'{source} still has old PIDs as ACTIVE')
            invalid_fact_maps_df.show(20, False)
            self.valid = False
        else:
            print("OLD_PID_INACTIVE VALIDATION SUCCEEDED for", source)


    def validate_fact_counts(self, source, fact_maps_df:DataFrame, counts):
        final_fact_maps_count, old_fact_maps_count, source_maps_count = counts

        fact_gen_stats_file = os.path.join(self.DATA_DIR, "DATAGEN", "STATS", f"{source.lower()}.json")
        if self.glob(fact_gen_stats_file):
            fact_gen_stats = self.get_json_data(fact_gen_stats_file)
            total_datagen_rows = fact_gen_stats.get("total_fact_rows", 0)
            total_fact_maps_rows = fact_maps_df.filter(F.col("VERSION").isin(float(self.RUN_VERSION))).count()

        if self.ees_full_update():
            old_fact_maps_stats_file = self.last_data_dir("FACT_MAPS_VERSIONED", "STATS", f"{source.lower()}.json")
            old_fact_maps_stats = self.get_json_data(old_fact_maps_stats_file)
            old_active_count = old_fact_maps_stats["fact_row_active"]
            current_fact_maps_active_count = fact_maps_df.filter(F.col('UPDATED_BY')==0.0).count()

        if self.glob(fact_gen_stats_file) and total_datagen_rows != total_fact_maps_rows:
            print(f'{source} has a mismatch in row count, expected(total_datagen_rows) = {total_datagen_rows}, '
                  f'actual(total_fact_maps_rows) = {total_fact_maps_rows}')
            self.valid = False

        elif final_fact_maps_count != (old_fact_maps_count + source_maps_count):
            print(f"{final_fact_maps_count} rows in final output, expected {old_fact_maps_count + source_maps_count}")
            self.valid = False

        elif self.ees_full_update() and old_active_count != current_fact_maps_active_count:
            print(f"{old_active_count} active rows in old fact maps, expected {current_fact_maps_active_count}")
            self.valid = False

        else:
            print(f"{old_fact_maps_count} + {source_maps_count} = {final_fact_maps_count}")
            print("FACT_COUNT VALIDATION SUCCEEDED for", source)


    def generate_version_counts(self, fact_maps_df: DataFrame, out_maps_dir):
        count_df = fact_maps_df.groupBy("VERSION", "UPDATED_BY").agg(F.count("*").alias("COUNT")).cache()
        count_file = os.path.join(out_maps_dir, "_version.count")
        self.write_df_to_csv(count_df, count_file)
        count_df = count_df.groupBy("VERSION").agg(F.sum("COUNT").alias("COUNT"))
        count_df.show(20, False)


    def log_version_stats(self, source, fact_maps_df:DataFrame, final_count, start, key_update_count=None):
        counter = Counter()
        counter.update({"fact_rows_all_versions": final_count})
        counter.update({"fact_rows_curr_version": fact_maps_df.filter(F.col("VERSION").isin(float(self.RUN_VERSION))).count()})
        counter.update({"fact_row_updates": fact_maps_df.filter(F.col("UPDATED_BY").isin(float(self.RUN_VERSION))).count()})
        counter.update({"fact_row_active": fact_maps_df.filter(F.col("UPDATED_BY").isin(0.0)).count()})
        if key_update_count is not None:
            counter.update({"key_update_count": key_update_count})
        counter.update({"run_time": time.perf_counter() - start})
        self.log_spark_stats("FACT_MAPS_VERSIONED", source, counter)

    def exists_in_harmonized_interim(self, source):
        path = os.path.join(self.ROOT_DIR, self.RUN_VERSION, "HARMONIZED_INTERIM", source.upper())
        print(f'Checking for file in {path}')
        return self.check_file_exists(path)

    def gen_max_row_id(self, source_list):
        begin = time.perf_counter()
        failures = []
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        data_dir = os.path.join(self.WRITE_ROOT_DIR, run_version)
        for source in source_list:
            self.init_spark_session(source)
            fact_maps_dir = self.last_data_dir("FACT_MAPS_VERSIONED", source,run_version=run_version)
            if not fact_maps_dir:
                print(f"FACT MAPS VERSIONED {source} does not exist")
                continue

            out_dir = os.path.join(data_dir, "MAX_ROW_ID", source)
            if self.skip_if_exists and self.glob(out_dir):
                print(f'{source} already exists. Skipping')
                continue

            if self.ees_full_update():
                if source not in self.extraction_configs.get("tables", {}):
                    print(f"Not an EES table. Skipping {source}")
                    continue
                if not self.exists_in_harmonized_interim(source):
                    print(f"No entry within HARMONIZED_INTERIM for {source.upper()} skipping...")
                    continue
            else:
                input_source_dir = os.path.join(self.ROOT_SOURCE_DIR, run_version)
                source_path = os.path.join(input_source_dir, source)
                if not self.glob(source_path):
                    print(f"No data at {source_path}, skipping...")
                    continue

            fact_maps_df = self.read_final_dir(fact_maps_dir)
            fact_maps_df = self.assign_file_id_for_nfer_pid(fact_maps_df, source)
            fact_maps_df = fact_maps_df.groupBy("FILE_ID").agg(F.max("ROW_ID").alias("MAX_ROW_ID"))
            self.df_to_parquet(fact_maps_df, out_dir, ["FILE_ID", "MAX_ROW_ID"])

        end = time.perf_counter()
        print("MAX_ROW_ID: Total Time =", end - begin)
        if failures:
            raise Exception("MAX_ROW_ID: Failures =", failures)


    def map_version_directories(self):
        if not os.path.exists(self.LOCAL_ROOT_DIR):
            raise Exception(f"Local root directory {self.LOCAL_ROOT_DIR} does not exist")

        run_version = self.options.version if self.options.version else self.RUN_VERSION
        source_list = sorted([k for k in self.SYSTEM_DICT['data_tables'].keys() if not k.endswith('_BRIDGE')])

        version_dir_map = {
            "data_disc": "/" + self.LOCAL_ROOT_DIR.split("/")[1],
            "root_dir": self.LOCAL_ROOT_DIR,
            "DATAGEN/DIM_MAPS/patient_map.json": os.path.join(self.LOCAL_ROOT_DIR, "NFERX/DATAGEN/DIM_MAPS", "patient_map.json"),
        }

        data_dirs = ["DATAGEN/DIM_INFO"]
        for data_dir in data_dirs:
            for version in self.VERSION_LIST:
                data_dir_path = os.path.join(self.LOCAL_ROOT_DIR, version, data_dir)
                if float(version) > float(run_version) or not self.glob(data_dir_path):
                    continue
                version_dir_map.setdefault(data_dir, os.path.join(version, data_dir))
                print(f"Adding {data_dir} for version {version}")
                break

        for source in source_list:
            source = source.upper()
            source_dict = version_dir_map.setdefault(source, {})
            datagen_list = source_dict.setdefault("DATAGEN", [])
            for version in self.VERSION_LIST:
                datagen_dir = os.path.join(self.LOCAL_ROOT_DIR, version, "DATAGEN", source)
                if float(version) > float(run_version) or not self.glob(datagen_dir):
                    continue
                datagen_list.append(os.path.join(version, "DATAGEN", source))
                if "VERSION_MAPS" not in source_dict:
                    version_maps_dir = os.path.join(self.LOCAL_ROOT_DIR, version, "FACT_MAPS_VERSIONED", source)
                    if source == self.DIM_PATIENT:
                        version_maps_dir = os.path.join(self.LOCAL_ROOT_DIR, version, "DATAGEN", "DIM_MAPS_VERSIONED")
                    if not self.glob(version_maps_dir):
                        print(f"WARNING: Version maps directory does not exist: {version_maps_dir}")
                    source_dict["VERSION_MAPS"] = version_maps_dir.replace(self.LOCAL_ROOT_DIR, "")

        syn_dir_dict = version_dir_map.setdefault("SYN_DATA", {})
        for syn_config in self.SYSTEM_DICT["syn_tables"].values():
            syn_dir = syn_config.get("dir_name")
            if not syn_dir or not syn_dir.startswith("SYN_DATA"):
                continue
            for version in self.VERSION_LIST:
                syn_dir_path = os.path.join(self.LOCAL_ROOT_DIR, version, syn_dir)
                if float(version) > float(run_version) or not self.glob(syn_dir_path):
                    continue
                syn_dir_dict.setdefault(syn_dir.split("/")[-1], os.path.join(version, syn_dir))
                break

        version_dir_map_path = os.path.join(self.LOCAL_ROOT_DIR, "RUN_STATE", run_version, "version_dir_map.json")
        os.makedirs(os.path.dirname(version_dir_map_path), exist_ok=True)
        self.dump_json_data(version_dir_map_path, version_dir_map)
        print(f"Version Directory Map written to {version_dir_map_path}")


    def parquet_to_final(self, source_list):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        for source in source_list:
            print(f'SOURCE: {source}')
            source_dir = os.path.join(self.ROOT_DIR, version, "FACT_MAPS_VERSIONED.parquet", source)
            if not self.glob(source_dir):
                print("No source file found for ", source)
                continue

            out_dir = os.path.join(self.LOCAL_ROOT_DIR, version, "FACT_MAPS_VERSIONED", source)
            if self.skip_if_exists and self.glob(os.path.join(out_dir, "header.csv")):
                print(f"Already exists, hence skipping {source}")
                continue

            start = time.perf_counter()
            self.init_spark_session(source)
            source_df = self.read_parquet_to_df(source_dir)
            self.df_to_patient_blocks(source_df, out_dir, source_df.columns, source)

            src_version_count = os.path.join(self.ROOT_DIR, version, "FACT_MAPS_VERSIONED.parquet", source, "_version.count")
            if self.glob(src_version_count):
                self.copy_file(src_version_count, os.path.join(out_dir, "_version.count"))

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("PARQUET_TO_FINAL: Total Time =", end - begin)



if __name__ == '__main__':
    FactVersionJob().run()
