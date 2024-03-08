import os
import time
from collections import Counter


from pyspark.sql import *
from pyspark.sql import functions as F

from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator
from core.config_vars import INCR_COUNTER

SELECT_COLS = ["NFER_PID", "PATIENT_DK", "PATIENT_CLINIC_NUMBER"]


class DimPatientProcessor(Orchestrator):

    def __init__(self):
        super().__init__()
        self.filter_only_updates = False

    def run(self):
        if self.options.run == 'dim_patient':
            self.process_dim_patient(process_age_time=True)
        elif self.options.run == 'dim_patient_without_age_time':
            self.process_dim_patient(process_age_time=False)
        elif self.options.run == 'delta_table':
            self.gen_delta_table()
        elif self.options.run == 'version_table':
            self.gen_version_delta_table()

    def process_dim_patient(self, process_age_time=True):
        begin = time.perf_counter()
        source = self.DIM_PATIENT
        self.init_spark_session(source)
        amc_schema, nfer_schema, _ = self.read_schema(source)
        timestamp_fields = self.get_timestamp_fields(source)

        if self.skip_if_exists and self.data_dir("DATAGEN", source):
            print(f"{self.DIM_PATIENT} already processed, skipping.")
            return

        patient_map_df, patient_map_schema = self.load_patient_maps()
        patient_map_df = patient_map_df.cache()
        raw_dim_patient_df = self.raw_data_reader.read_raw_data(source, version=self.RUN_VERSION)

        dim_patient_df = raw_dim_patient_df.withColumnRenamed("FILE_ID", "ORIG_FILE_ID")
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)

        if process_age_time:
            dim_patient_df = self.calculate_encounter_epoch_and_dob(dim_patient_df)
            dim_patient_df = self.process_timestamp_fields(dim_patient_df, timestamp_fields)

        dim_patient_df = self.generate_nfer_pid(dim_patient_df, patient_map_df)
        dim_patient_df = self.generate_patient_dkid(dim_patient_df, patient_map_df)
        dim_patient_df = self.assign_file_id_for_nfer_pid(dim_patient_df, source)
        dim_patient_df = self.assign_rowid(dim_patient_df, patient_map_df)
        final_patient_df = dim_patient_df.cache()
        if self.SAMPLE_RUN:
            final_patient_df = self.perform_sampling(final_patient_df)
        patient_map_df = self.update_patient_map_version(final_patient_df, patient_map_df)
        final_patient_map_df = self.create_version_for_new_patients(final_patient_df, patient_map_df, patient_map_schema)

        final_patient_map_df.cache()
        print("Validating NFER_PID uniqueness")
        self.validate_nfer_pid(final_patient_map_df)

        print("Capturing CLINIC_NO_CHANGE patients")
        clinic_no_change_count = self.capture_clinic_no_change_patients(final_patient_df, patient_map_df)

        print("Generating dim_patient data")
        self.write_final_dir("DATAGEN", final_patient_df, nfer_schema, self.DIM_PATIENT.upper())
        self.df_to_pre_processed_parquet(source, final_patient_df, amc_schema)
        self.write_final_dir("DATAGEN", final_patient_map_df, patient_map_schema, "DIM_MAPS_VERSIONED")

        total_time = time.perf_counter() - begin
        self.log_counters(final_patient_map_df, clinic_no_change_count, total_time)
        print("DIM PATIENT: Total Time =", total_time)

    def perform_sampling(self, final_patient_df):
        sample_dim_df = self.read_csv_to_df(self.SAMPLE_PID_SOURCE)
        if not sample_dim_df and self.RUN_MODE == RunMode.Full.value and not os.path.exists(self.SAMPLE_PID_SOURCE):
            sub_final_df = final_patient_df.filter(F.col(self.SAMPLE_COL).isin(self.SAMPLE_MANUAL_ENTRIES))
            final_patient_df = final_patient_df.orderBy(F.col(self.SAMPLE_COL)).limit(self.SAMPLE_MAX_SIZE-len(self.SAMPLE_MANUAL_ENTRIES))
            final_patient_df = final_patient_df.union(sub_final_df)
            pid_df = final_patient_df.select(SELECT_COLS)
            self.write_df_to_csv(pid_df, self.SAMPLE_PID_SOURCE)
        else:
            sample_dim_df = sample_dim_df.limit(self.SAMPLE_SIZE)
            sample_dim_df = final_patient_df.join(sample_dim_df.select(self.SAMPLE_COL), [self.SAMPLE_COL])
            sample_df_pids = sample_dim_df.select(F.col(self.SAMPLE_COL)).rdd.flatMap(lambda x: x).collect()
            filter_df = final_patient_df.filter(~F.col(self.SAMPLE_COL).isin(sample_df_pids))
            final_patient_df = sample_dim_df.unionByName(filter_df.orderBy(F.col(self.SAMPLE_COL)).limit(30))
        return final_patient_df

    def de_dupe_patient_meta(self, raw_source_df: DataFrame):
        if not self.PATIENT_MERGED_FLAG:
            return raw_source_df

        source_df = raw_source_df.withColumn(self.PATIENT_CLINIC_NBR,
                F.concat(F.col(self.PATIENT_CLINIC_NBR), F.lit("|"), F.col(self.PATIENT_MERGED_FLAG)))
        conflict_dk_df = source_df.groupBy(self.PATIENT_DK).agg(
            F.min(self.PATIENT_CLINIC_NBR).alias("CLINIC_NBR_1"),
            F.max(self.PATIENT_CLINIC_NBR).alias("CLINIC_NBR_2")
        ).filter(F.col("CLINIC_NBR_1") != F.col("CLINIC_NBR_2")).cache()

        conflict_dk_count = conflict_dk_df.count()
        if conflict_dk_count > 0:
            conflict_dk_df.show(20, truncate=False)
            print(f"{conflict_dk_count} PATIENT_DK have conflicting clinic numbers, dropping duplicates with Merged-Flag=N")

        window = Window.partitionBy(self.PATIENT_DK).orderBy([F.desc(INCR_COUNTER), F.desc(self.PATIENT_MERGED_FLAG)])
        raw_source_df = raw_source_df.withColumn("ROW_NUM", F.row_number().over(window))
        raw_source_df = raw_source_df.filter(F.col("ROW_NUM") == 1)
        raw_source_df = raw_source_df.drop("ROW_NUM")
        return raw_source_df

    def calculate_encounter_epoch_and_dob(self, dim_patient_df: DataFrame):
        dim_patient_df = dim_patient_df.withColumn("AGE",
            F.when(F.col("AGE_AT_FIRST_ENCOUNTER").isin("89+"), F.lit(90))
            .otherwise(F.col("AGE_AT_FIRST_ENCOUNTER").cast("INT"))
        )
        dim_patient_df = dim_patient_df.withColumn(self.ENCOUNTER_EPOCH, F.unix_timestamp("FIRST_ENCOUNTER_DATE", "yyyy-MM-dd"))
        dim_patient_df = dim_patient_df.withColumn(self.BIRTH_DATE, F.expr("add_months(FIRST_ENCOUNTER_DATE, AGE * -12)"))
        dim_patient_df = dim_patient_df.withColumn(self.BIRTH_DATE, F.date_format(self.BIRTH_DATE, "yyyy-MM-dd"))
        return dim_patient_df

    def process_timestamp_fields(self, raw_dim_patient_df: DataFrame, timestamp_fields):
        for field in timestamp_fields:
            if field != self.ENCOUNTER_EPOCH:
                raw_dim_patient_df = self.parse_timestamp(raw_dim_patient_df, field)
        return raw_dim_patient_df

    def load_patient_maps(self):
        patient_map_dir = self.last_data_dir("DATAGEN", "DIM_MAPS_VERSIONED")
        if not patient_map_dir and self.OLD_MAPS_DIR:
            patient_map_dir = os.path.join(self.OLD_MAPS_DIR, "DIM_MAPS_VERSIONED")

        nfer_schema = self.SYSTEM_DICT["version_tables"]["dim_maps_versioned"]["nfer_schema"]
        type_schema = self.SYSTEM_DICT["version_tables"]["dim_maps_versioned"]["type_schema"]
        if patient_map_dir and self.glob(patient_map_dir):
            patient_map_df = self.read_final_dir(patient_map_dir)
        else:
            patient_map_df = self.create_empty_dataframe(nfer_schema, type_schema)

        if "FILE_ID" not in patient_map_df.columns:
            patient_map_df = self.assign_file_id_for_nfer_pid(patient_map_df, self.DIM_PATIENT)

        return patient_map_df, nfer_schema

    def generate_nfer_pid(self, dim_patient_df: DataFrame, patient_map_df: DataFrame):
        nfer_pid_maps_df = patient_map_df.select(self.PATIENT_CLINIC_NBR, "NFER_PID").distinct()
        dim_patient_df = dim_patient_df.join(F.broadcast(nfer_pid_maps_df), self.PATIENT_CLINIC_NBR, "left")
        dim_patient_df = dim_patient_df.na.fill(0, subset=["NFER_PID"])
        max_nfer_pid = nfer_pid_maps_df.agg(F.max("NFER_PID")).collect()[0][0]
        if not max_nfer_pid:
            max_nfer_pid = 0
        order_window = Window.partitionBy().orderBy("NFER_PID", self.PATIENT_CLINIC_NBR)
        dim_patient_df = dim_patient_df.withColumn("NFER_PID",
            F.when(F.col("NFER_PID").isin(0), F.lit(max_nfer_pid) + F.dense_rank().over(order_window))
            .otherwise(F.col("NFER_PID")))
        return dim_patient_df

    def generate_patient_dkid(self, dim_patient_df: DataFrame, patient_map_df: DataFrame):
        pdk_id_maps_df = patient_map_df.filter(F.col("UPDATED_BY") == F.lit(0.0))
        pdk_id_maps_df = pdk_id_maps_df.select(self.PATIENT_DK, "PDK_ID").distinct()
        dim_patient_df = dim_patient_df.join(F.broadcast(pdk_id_maps_df), self.PATIENT_DK, "left")
        dim_patient_df = dim_patient_df.na.fill(0, subset=["PDK_ID"])

        max_pdk_id = pdk_id_maps_df.agg(F.max("PDK_ID")).collect()[0][0]
        if not max_pdk_id:
            max_pdk_id = 0
        order_window = Window.partitionBy().orderBy("PDK_ID", self.PATIENT_DK)
        dim_patient_df = dim_patient_df.withColumn("PDK_ID",
            F.when(F.col("PDK_ID").isin(0), F.lit(max_pdk_id) + F.dense_rank().over(order_window))
            .otherwise(F.col("PDK_ID")))
        return dim_patient_df

    def filter_updates_only(self, dim_patient_df: DataFrame, patient_map_df: DataFrame, amc_schema) -> DataFrame:
        row_hash_fields = [f for f in amc_schema if f != self.PATIENT_DK]
        raise NotImplementedError("Need to implement row_hash_fields")

    def assign_rowid(self, dim_patient_df: DataFrame, patient_map_df: DataFrame) -> DataFrame:
        if self.RUN_MODE == RunMode.Full.value:
            dim_patient_df = dim_patient_df.withColumn("ROW_ID", F.lit(0))
            dim_patient_df = dim_patient_df.withColumn("MAX_ROW_ID", F.lit(0))
        else:
            max_rowid_df = patient_map_df.groupBy("FILE_ID").agg(F.max("ROW_ID").alias("MAX_ROW_ID"))
            dim_patient_df = dim_patient_df.join(F.broadcast(max_rowid_df), ["FILE_ID"])
        window = Window.partitionBy("FILE_ID").orderBy("NFER_PID")
        dim_patient_df = dim_patient_df.withColumn("ROW_ID", F.col("MAX_ROW_ID") + F.row_number().over(window))
        return dim_patient_df

    def update_patient_map_version(self, final_patient_df: DataFrame, patient_map_df: DataFrame):
        if self.RUN_MODE == RunMode.Full.value:
            return patient_map_df
        patient_dk_df = final_patient_df.select(self.PATIENT_DK)
        patient_dk_df = patient_dk_df.withColumn("OLD_PATIENT", F.lit(True))
        patient_map_df = patient_map_df.join(F.broadcast(patient_dk_df), self.PATIENT_DK, "left")
        patient_map_df = patient_map_df.withColumn("UPDATED_BY",
            F.when((F.col("OLD_PATIENT") & F.col("UPDATED_BY").isin(0.0)), F.lit(float(self.RUN_VERSION)))
            .otherwise(F.col("UPDATED_BY"))
        )
        return patient_map_df

    def create_version_for_new_patients(self, final_patient_df: DataFrame, old_patient_maps_df: DataFrame, patient_map_schema):
        new_patient_maps_df = final_patient_df.withColumn("VERSION", F.lit(float(self.RUN_VERSION)))
        new_patient_maps_df = new_patient_maps_df.withColumn("UPDATED_BY", F.lit(0.0))
        new_patient_maps_df = new_patient_maps_df.withColumn("SMALL_FILE_ID", F.lit(0))
        new_patient_maps_df = new_patient_maps_df.select(["FILE_ID"] + patient_map_schema)
        if self.RUN_MODE == RunMode.Full.value:
            return new_patient_maps_df
        old_patient_maps_df = old_patient_maps_df.select(["FILE_ID"] + patient_map_schema)
        return old_patient_maps_df.union(new_patient_maps_df)

    def capture_clinic_no_change_patients(self, final_patient_df: DataFrame, patient_map_df: DataFrame):
        if self.RUN_MODE == RunMode.Full.value:
            return
        changed_fields = [self.PATIENT_CLINIC_NBR, "NFER_PID","PDK_ID"]
        new_dim_patient_df = final_patient_df.select([self.PATIENT_DK] + changed_fields)

        updated_maps_df = patient_map_df.filter(F.col("UPDATED_BY") == F.lit(float(self.RUN_VERSION)))
        for field in changed_fields:
            new_dim_patient_df = new_dim_patient_df.withColumnRenamed(field, f"NEW_{field}")
            updated_maps_df = updated_maps_df.withColumnRenamed(field, f"OLD_{field}")

        updated_maps_df = updated_maps_df.join(F.broadcast(new_dim_patient_df), [self.PATIENT_DK])
        clinic_no_change_df = updated_maps_df.filter(F.col("NEW_PATIENT_CLINIC_NUMBER") != F.col("OLD_PATIENT_CLINIC_NUMBER"))
        schema = [
            self.PATIENT_DK,
            "NEW_NFER_PID", "OLD_NFER_PID","OLD_PDK_ID",
            "NEW_PATIENT_CLINIC_NUMBER", "OLD_PATIENT_CLINIC_NUMBER",
            "VERSION", "UPDATED_BY"
        ]
        clinic_no_change_df = clinic_no_change_df.select(schema).cache()
        clinic_no_change_count = clinic_no_change_df.count()
        clinic_no_change_file = os.path.join(self.DATAGEN_DIR, "DIM_MAPS", "clinic_no_change.txt")
        self.write_df_to_csv(clinic_no_change_df, clinic_no_change_file)
        return clinic_no_change_count

    def validate_nfer_pid(self, patient_map_df: DataFrame):
        pid_collision_df = patient_map_df.groupBy(self.PATIENT_CLINIC_NBR).agg(
            F.min("NFER_PID").alias("MIN_NFER_PID"),
            F.max("NFER_PID").alias("MAX_NFER_PID"),
        ).filter(F.col("MIN_NFER_PID") != F.col("MAX_NFER_PID")).cache()
        pid_collision_count = pid_collision_df.count()
        if pid_collision_count > 0:
            print(f"PID collision count: {pid_collision_count}")
            pid_collision_df.show(10, truncate=False)
            raise ValueError(f"NFER_PID or PATIENT_CLINIC_NUMBER is not unique")

        cno_collision_df = patient_map_df.groupBy("NFER_PID").agg(
            F.min(self.PATIENT_CLINIC_NBR).alias("MIN_PATIENT_CLINIC_NBR"),
            F.max(self.PATIENT_CLINIC_NBR).alias("MAX_PATIENT_CLINIC_NBR"),
        ).filter(F.col("MIN_PATIENT_CLINIC_NBR") != F.col("MAX_PATIENT_CLINIC_NBR")).cache()
        cno_collision_count = cno_collision_df.count()
        if cno_collision_count > 0:
            print(f"CNO collision count: {cno_collision_count}")
            cno_collision_df.show(10, truncate=False)
            raise ValueError(f"NFER_PID or PATIENT_CLINIC_NUMBER is not unique")

    def log_counters(self, final_patient_maps_df: DataFrame, clinic_no_change_count, run_time) -> Counter:
        counter = Counter({
            "total_active_patient_dk": 0,
            "total_active_clinic_no": 0,
            "new_patient_dk": 0,
            "new_clinic_no": 0,
            "clinic_no_merged": clinic_no_change_count,
            "run_time": run_time
        })

        new_count_df = final_patient_maps_df.filter(F.col("VERSION") == F.lit(float(self.RUN_VERSION))).groupBy().agg(
            F.countDistinct(self.PATIENT_DK).alias("NEW_PATIENT_DK_COUNT"),
            F.countDistinct(self.PATIENT_CLINIC_NBR).alias("NEW_CLINIC_NO_COUNT"),
        )
        for row in new_count_df.collect():
            counter.update({
                "new_patient_dk": row["NEW_PATIENT_DK_COUNT"],
                "new_clinic_no": row["NEW_CLINIC_NO_COUNT"]
            })

        total_count_df = final_patient_maps_df.filter(F.col("UPDATED_BY") == F.lit(0.0)).groupBy().agg(
            F.countDistinct(self.PATIENT_DK).alias("TOTAL_PATIENT_DK_COUNT"),
            F.countDistinct(self.PATIENT_CLINIC_NBR).alias("TOTAL_CLINIC_NO_COUNT"),
        )
        for row in total_count_df.collect():
            counter.update({
                "total_active_patient_dk": row["TOTAL_PATIENT_DK_COUNT"],
                "total_active_clinic_no": row["TOTAL_CLINIC_NO_COUNT"]
            })

        self.log_spark_stats("DATAGEN", self.DIM_PATIENT, counter)

    def gen_delta_table(self):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        print("Generating FACT Delta Tables for ", version)
        source = self.DIM_PATIENT
        print(f'SOURCE: {source}')
        self.init_spark_session(source)
        nfer_schema = self.read_schema(source)[1] + [self.PATIENT_DK, self.PATIENT_CLINIC_NBR, "VERSION", "UPDATED_BY"]
        source_df = self.read_versioned_datagen_dir(source, columns=nfer_schema, latest_only=False, version=version, force_csv_read=True)
        source_df = source_df.drop("FILE_ID")
        self.release_versioned_delta_table(source_df, source, version, write_latest_delta=True)
        end = time.perf_counter()
        print("SPARK_SQL: Total Time =", end - begin)

    def gen_version_delta_table(self):
        self.init_spark_session()
        version_meta = os.path.join(self.RUN_STATE_DIR, 'data_versions.txt')
        if self.SAMPLE_RUN and not os.path.exists(version_meta):
            return
        vdf = self.read_csv_to_df(version_meta)
        delta_table_path = os.path.join(self.DATA_DIR, "DELTA_TABLES/META_TABLES/DATA_VERSIONS.parquet")
        self.write_df_to_delta_lake(vdf, delta_table_path)


if __name__ == '__main__':
    DimPatientProcessor().run()
