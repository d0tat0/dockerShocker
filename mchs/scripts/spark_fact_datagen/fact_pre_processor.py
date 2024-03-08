import json
import os
import time

from pyspark.sql import *
from pyspark.sql import functions as F
from spark_jobs.orchestrator import Orchestrator
from core.config_vars import RunMode, INCR_COUNTER


PATIENT_MAP_DF = None


class FactPreProcessJob(Orchestrator):
    def __init__(self):
        super().__init__()
        self.parent = self
        self.process_fact_only_patients = False
        self.check_false_data_updates = [] #["FACT_LAB_TEST"]
        self.filter_out = []

    def run(self):
        table_list = self.parse_options_source()
        if 'preprocess' in self.options.run:
            self.pre_process_main(table_list)
        if 'raw_stats' in self.options.run:
            self.pre_process_main(table_list, stats_only_mode=True)
        if 'validate_stats' in self.options.run:
            self.validate_pre_process_stats(table_list)

    def pre_process_main(self, source_list, stats_only_mode=False):
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        self.init_spark_session()
        patient_map_df = self.read_versioned_dim_maps(version=run_version)
        for source in source_list:
            start = time.perf_counter()
            os.path.join(self.INPUT_SOURCE_DIR, source)
            source_out_dir = os.path.join(self.ROOT_SOURCE_DIR, run_version, source)
            stats_file = os.path.join(self.ROOT_SOURCE_DIR, run_version, "STATS", f"{source.lower()}.json")
            if self.skip_if_exists:
                if self.glob(source_out_dir) and self.glob(stats_file):
                    print(source, "Already processed, skipping")
                    continue

            self.init_spark_session(source)
            amc_schema, nfer_schema, type_schema, unique_schema, dedupe_sort_schema = self.read_config(source)
            raw_source_df = self.raw_data_reader.read_raw_data(source, version=run_version)
            if raw_source_df is None:
                print(f'No raw data available for {source}')
                continue

            if source in self.check_false_data_updates:
                print(f'Filtering out false data updates for {source}')
                raw_source_df = self.filter_out_false_data_updates(source, raw_source_df)

            raw_source_df = raw_source_df.withColumnRenamed("FILE_ID", "ORIG_FILE_ID")
            raw_source_df = raw_source_df.select([f for f in amc_schema if f in raw_source_df.columns] + [INCR_COUNTER])
            raw_source_df = self.pre_process_raw_source(source, raw_source_df, patient_map_df, unique_schema, dedupe_sort_schema, run_version)
            pre_process, calc_stats = self.define_pre_process_mode(source, stats_only_mode, stats_file)

            if calc_stats and not pre_process:
                raw_source_df = raw_source_df.select(["INDICATOR", self.PATIENT_DK])

            if calc_stats:
                raw_source_df.cache()
                if raw_source_df.count() == 0:
                    print(f'No data available for {source}')
                    continue

            if pre_process:
                if not self.skip_if_exists or not self.glob(source_out_dir):
                    source_df = raw_source_df.filter(~F.col("INDICATOR").isin(self.filter_out))
                    self.df_to_pre_processed_parquet(source, source_df, amc_schema, out_dir=source_out_dir)

            if calc_stats:
                self.dump_stats(raw_source_df, stats_file)
                if self.process_fact_only_patients:
                    self.dump_fact_only_patients(raw_source_df, source, run_version)

            time_taken = time.perf_counter() - start
            print(source, ": Total Time =", time_taken)
            self.SPARK_SESSION.catalog.clearCache()


    def read_config(self, source):
        amc_schema, nfer_schema, type_schema = self.read_schema(source)
        unique_schema = self.SYSTEM_DICT['data_tables'][source.lower()]["unique_indices"]
        dedupe_sort_schema = self.SYSTEM_DICT['data_tables'][source.lower()].get("dedupe_sort_indices", [])
        return amc_schema, nfer_schema, type_schema, unique_schema, dedupe_sort_schema


    def define_pre_process_mode(self, source, stats_only_mode, stats_file):
        pre_process = not stats_only_mode
        calc_stats = stats_only_mode or source not in self.BIG_TABLES or self.RUN_MODE == RunMode.Delta.value
        calc_stats = calc_stats and (not self.skip_if_exists or not os.path.exists(stats_file))
        return pre_process, calc_stats

    def pre_process_raw_source(self, source, raw_source_df: DataFrame, patient_map_df: DataFrame, unique_schema, dedupe_sort_schema, run_version) -> DataFrame:
        if self.RUN_MODE == RunMode.Delta.value:
            raw_source_df = self.reprocess_old_patients(source, raw_source_df, run_version)
        if self.SAMPLE_RUN:
            raw_source_df = self.sample_patients(raw_source_df, patient_map_df)
        raw_source_df = self.assign_nfer_pid(raw_source_df, patient_map_df)
        raw_source_df = self.assign_file_id_for_nfer_pid(raw_source_df, source)
        raw_source_df = self.calc_key_hash(raw_source_df, unique_schema)
        raw_source_df = self.detect_duplicates(source, raw_source_df, dedupe_sort_schema)
        raw_source_df = self.assign_drop_indicator(raw_source_df)
        return raw_source_df

    def sample_patients(self, raw_source_df, patient_map_df):
        # Valid when SAMPLE_RUN is true. This will assign PID and drop FACT only patients at this step only.
        raw_source_df = self.assign_nfer_pid(raw_source_df, patient_map_df)
        raw_source_df = raw_source_df.filter(F.col("NFER_PID") != 0)
        return raw_source_df

    def reprocess_old_patients(self, source, raw_source_df: DataFrame, run_version) -> DataFrame:
        reprocessed_pdk_df = self.read_patients_for_reprocessing(source, run_version).cache()
        re_processed_pdk_count = reprocessed_pdk_df.count()
        print(f"Reprocessing {re_processed_pdk_count} patients for {source}")
        raw_source_df = raw_source_df.withColumn("ROW_ID", F.lit(0))
        if re_processed_pdk_count > 0:
            for version in self.VERSION_LIST[1:]:
                if self.ees_full_update(version):
                    continue
                old_source_df = self.read_source_files(source, version=version)
                if old_source_df is None:
                    continue
                old_source_df = old_source_df.join(F.broadcast(reprocessed_pdk_df), [self.PATIENT_DK])
                old_source_df = old_source_df.withColumn(INCR_COUNTER, F.lit(0))
                old_source_df = old_source_df.select(raw_source_df.columns)
                raw_source_df = raw_source_df.union(old_source_df)
        return raw_source_df

    def filter_out_false_data_updates(self, source, raw_source_df:DataFrame) -> DataFrame:
        mayo_schema, nfer_schema, _, timestamp_fields = self.read_schema(source)
        unique_indices = self.read_unique_schema(source)
        row_hash_fields = [f for f in mayo_schema if f in nfer_schema and f not in ["ORIG_FILE_ID", "ROW_ID"]]
        old_raw_source_df = None
        for old_version in self.VERSION_LIST[1:]:
            old_raw_df = self.read_source_files(source, version=old_version)
            if old_raw_df is None:
                continue

            for field in timestamp_fields:
                old_raw_df = self.parse_timestamp(old_raw_df, field, old_version)

            old_raw_df = self.calc_key_hash(old_raw_df, unique_indices, "KEY_HASH")
            old_raw_df = self.calc_key_hash(old_raw_df, row_hash_fields, "OLD_ROW_HASH")

            old_raw_df = old_raw_df.select(["KEY_HASH", "OLD_ROW_HASH"])
            old_raw_source_df = old_raw_df if old_raw_source_df is None else old_raw_source_df.union(old_raw_df)

        raw_source_df = self.calc_key_hash(raw_source_df, row_hash_fields, "NEW_ROW_HASH")
        raw_source_df = raw_source_df.join(old_raw_source_df, on="KEY_HASH", how="LEFT")
        raw_source_df = raw_source_df.withColumn("FALSE_UPDATE", F.col("OLD_ROW_HASH") == F.col("NEW_ROW_HASH"))
        raw_source_df = raw_source_df.filter(F.col("FALSE_UPDATE") == False)
        raw_source_df = raw_source_df.drop("OLD_ROW_HASH", "NEW_ROW_HASH", "FALSE_UPDATE")
        return raw_source_df

    def assign_nfer_pid(self, raw_source_df: DataFrame, patient_map_df: DataFrame)-> DataFrame:
        if "NFER_PID" in raw_source_df.columns:
            return raw_source_df
        patient_map_df = patient_map_df.select([self.PATIENT_DK, "NFER_PID"])
        raw_source_df = raw_source_df.join(F.broadcast(patient_map_df), [self.PATIENT_DK], "LEFT")
        current_cols = raw_source_df.columns
        for _field in self.ADDITIONAL_PDK:
            if _field not in current_cols:
                continue
            new_field = _field.replace(self.PATIENT_DK, "NFER_PID")
            patient_map_df = patient_map_df.withColumnRenamed("NFER_PID", new_field).withColumnRenamed(self.PATIENT_DK, _field)
            raw_source_df = raw_source_df.join(F.broadcast(patient_map_df), _field, "LEFT")
        raw_source_df = raw_source_df.na.fill(0, ["NFER_PID"])
        return raw_source_df

    def detect_duplicates(self, source, raw_source_df: DataFrame, dedupe_sort_schema) -> DataFrame:
        order_schema = ["KEY_HASH"] + [INCR_COUNTER] + dedupe_sort_schema
        file_window = Window.partitionBy("FILE_ID").orderBy([F.desc_nulls_last(f) for f in order_schema])
        condition = F.col("KEY_HASH") == F.lag("KEY_HASH", 1, "").over(file_window)
        if source == "FACT_RHC_MEASUREMENTS":
            condition = (F.col("KEY_HASH") == F.lag("KEY_HASH", 1, "").over(file_window)) & (F.col(self.PATIENT_DK) == F.lag(self.PATIENT_DK, 1, "").over(file_window))
        raw_source_df = raw_source_df.withColumn("DUPLICATE", condition)
        return raw_source_df

    def assign_drop_indicator(self, raw_source_df: DataFrame) -> DataFrame:
        if self.DEID_VALIDATION_COLUMN not in raw_source_df.columns:
            raw_source_df = raw_source_df.withColumn(self.DEID_VALIDATION_COLUMN, F.lit("0"))
        if "ROW_ID" not in raw_source_df.columns:
            raw_source_df = raw_source_df.withColumn("ROW_ID", F.lit("0"))
        raw_source_df = raw_source_df.withColumn("INDICATOR",
            F.when(F.col(self.DEID_VALIDATION_COLUMN).startswith("NFER_VALIDATION"), "NFER_VALIDATION")
            .when(F.col("DUPLICATE") & F.col("ROW_ID").isin(0), F.lit("RAW_DUPLICATE"))
            .when(F.col("DUPLICATE") & ~F.col("ROW_ID").isin(0), F.lit("RE_PROCESSED_DUPLICATE"))
            .when(F.col("NFER_PID").isin(0), F.lit("FACT_ONLY_PATIENT"))
            .when(~F.col("ROW_ID").isin(0), F.lit("RE_PROCESSED_RECORDS"))
            .otherwise(F.lit("RAW_RECORDS"))
        )
        self.filter_out = ["NFER_VALIDATION", "RAW_DUPLICATE", "RE_PROCESSED_DUPLICATE", "FACT_ONLY_PATIENT"]
        return raw_source_df

    def dump_stats(self, raw_source_df: DataFrame, stats_file):
        comprehensive_stats = {
            "INPUT": {"RAW_TOTAL": 0, "RAW_DUPLICATE": 0, "RE_PROCESSED_TOTAL": 0, "RE_PROCESSED_DUPLICATE": 0},
            "OUTPUT": {"TOTAL": 0}
        }
        stats_df = raw_source_df.groupBy("INDICATOR").agg(F.count("INDICATOR").alias("COUNT"))
        for row in stats_df.collect():

            if row.INDICATOR in ["RAW_RECORDS"]:
                comprehensive_stats["INPUT"]["RAW_TOTAL"] += row.COUNT

            if row.INDICATOR in ["RAW_DUPLICATE"]:
                comprehensive_stats["INPUT"]["RAW_TOTAL"] += row.COUNT
                comprehensive_stats["INPUT"]["RAW_DUPLICATE"] = row.COUNT

            if row.INDICATOR in ["RE_PROCESSED_RECORDS"]:
                comprehensive_stats["INPUT"]["RE_PROCESSED_TOTAL"] += row.COUNT

            if row.INDICATOR in ["RE_PROCESSED_DUPLICATE"]:
                comprehensive_stats["INPUT"]["RE_PROCESSED_TOTAL"] += row.COUNT
                comprehensive_stats["INPUT"]["RE_PROCESSED_DUPLICATE"] = row.COUNT

            if row.INDICATOR not in self.filter_out:
                comprehensive_stats["OUTPUT"]["TOTAL"] += row.COUNT
                comprehensive_stats["OUTPUT"].setdefault(row.INDICATOR, row.COUNT)
            else:
                comprehensive_stats["INPUT"].setdefault(row.INDICATOR, row.COUNT)

        os.makedirs(os.path.dirname(stats_file), exist_ok=True)
        self.dump_json_data(stats_file, comprehensive_stats)
        print(json.dumps(comprehensive_stats, indent=4))


    def dump_fact_only_patients(self, raw_source_df:DataFrame, source, run_version):
        fact_only_patients_df = raw_source_df.filter(F.col("INDICATOR") == "FACT_ONLY_PATIENT")
        fact_only_patients_df = fact_only_patients_df.select([self.PATIENT_DK]).distinct()
        fact_only_patients_df.cache()
        if fact_only_patients_df.count() > 0:
            fact_only_patients_file = os.path.join(self.WRITE_ROOT_DIR, run_version, "FACT_ONLY_PATIENTS", source)
            self.df_to_parquet(fact_only_patients_df, fact_only_patients_file)


    def read_patients_for_reprocessing(self, source, run_version) -> DataFrame:
        patients_df = self.read_patients_merged(run_version)
        if self.process_fact_only_patients:
            fact_only_patients_df = self.read_fact_only_patients(source, run_version)
            patients_df = patients_df.union(fact_only_patients_df)
        patients_df = patients_df.distinct()
        return patients_df


    def read_patients_merged(self, run_version):
        clinic_no_change_file = os.path.join(self.ROOT_DIR, run_version, "DATAGEN/DIM_MAPS", "clinic_no_change.txt")
        if not self.glob(clinic_no_change_file):
            return self.SPARK_SESSION.createDataFrame([], schema=self.build_schema([self.PATIENT_DK], ["STRING"]))
        clinic_no_change_df = self.read_csv_to_df(clinic_no_change_file)
        updated_pdk_df = clinic_no_change_df.select([self.PATIENT_DK]).distinct()
        return updated_pdk_df


    def read_fact_only_patients(self, source, run_version):
        last_delta_version = self.get_last_delta_version(run_version)
        fact_only_patients_file = os.path.join(self.ROOT_DIR, last_delta_version, "FACT_ONLY_PATIENTS", source)
        if self.glob(fact_only_patients_file):
            available_patients_df = self.read_parquet_to_df(fact_only_patients_file)
            dim_maps_df = self.read_versioned_dim_maps([self.PATIENT_DK])
            available_patients_df = dim_maps_df.join(F.broadcast(available_patients_df), [self.PATIENT_DK])
            return available_patients_df
        return self.SPARK_SESSION.createDataFrame([], schema=self.build_schema([self.PATIENT_DK], ["STRING"]))


    def get_last_delta_version(self, run_version):
        for version in self.VERSION_LIST:
            if float(version) >= float(run_version) or self.ees_full_update(version):
                continue
            if self.glob(os.path.join(self.ROOT_DIR, version)):
                return version


    def validate_pre_process_stats(self, source_list):
        failures = set()
        for source in source_list:

            print("Source:", source)
            pre_processed_path = os.path.join(self.ROOT_SOURCE_DIR, self.RUN_VERSION, 'STATS')
            stats_file = os.path.join(pre_processed_path, f"{source.lower()}.json")
            if not self.glob(stats_file):
                print(f"Stats file {stats_file} does not exist")
                continue

            stats_data = self.get_json_data(stats_file)
            print("Stats Data:", json.dumps(stats_data, indent=4))

            duplicate_percent = int(stats_data["INPUT"].get("RAW_DUPLICATE", 0) * 100 / max(stats_data["INPUT"]["RAW_TOTAL"], 1))
            if duplicate_percent > self.MAX_THRESHOLD:
                print(f"Duplicate Percentage = {duplicate_percent}%, which is higher than threshold = {self.MAX_THRESHOLD}%")
                failures.add(source)

            fact_only_percent = int(stats_data["OUTPUT"].get("FACT_ONLY_PATIENT", 0) * 100 / max(stats_data["OUTPUT"]["TOTAL"], 1))
            if fact_only_percent > self.MAX_THRESHOLD:
                print(f"Fact Only Patient Percentage = {fact_only_percent}%, which is higher than threshold = {self.MAX_THRESHOLD}%")
                failures.add(source)

        if failures:
            raise Exception("PRE PROCESS VALIDATION FAILED for \n", "\n".join(failures))
        print("PRE_PROCESS VALIDATION SUCCEEDED.")


if __name__ == '__main__':
    FactPreProcessJob().run()