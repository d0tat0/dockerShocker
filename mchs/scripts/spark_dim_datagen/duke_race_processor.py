import os
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from spark_jobs.orchestrator import Orchestrator


# TODO Handle for incremental runs

class PatientRaceMerger(Orchestrator):
    def __init__(self):
        super().__init__()
        self.init_spark_session()

    def merge_race_data(self):
        patient_race_df = self.read_incremental_raw_data("PATIENT_RACE_LOOKUP")
        windowSpec = Window.partitionBy("NFER_PID")
        patient_race_df = patient_race_df.withColumn("count", F.count("NFER_PID").over(windowSpec))
        patient_race_df = patient_race_df.filter((F.col("MAPPED_CODES").isNotNull()) | (
            F.col("MAPPED_CODES.VALUECODING.DISPLAY") != "Unknown") | (F.col("count") < 2))
        patient_race_df = patient_race_df.withColumn("count", F.count("NFER_PID").over(windowSpec))
        patient_race_df = patient_race_df.withColumn("RACE_NAME", F.when((F.col("count") > 1), (
            F.lit("MULTI-RACE"))).otherwise(F.col("MAPPED_CODES.VALUECODING.DISPLAY")))

        patient_race_df = patient_race_df.groupBy("NFER_PID", "RACE_NAME").agg(
            F.collect_set("MAPPED_CODES").alias("MAPPED_CODES"))

        patient_race_df = patient_race_df.withColumn("RACE_NAME", F.when((F.size("MAPPED_CODES") > 1), F.concat(F.lit("MULTI-RACE: "), F.concat_ws(
            ",", F.col("MAPPED_CODES.VALUECODING.DISPLAY")))).otherwise(F.concat_ws(",", F.col("MAPPED_CODES.VALUECODING.DISPLAY"))))

        windowSpec = Window.orderBy("RACE_NAME")
        patient_race_df = patient_race_df.withColumn("SYN_RACE_DK", F.dense_rank().over(windowSpec))
        patient_race_df.cache()
        patient_race_df = patient_race_df.filter(F.col("SYN_RACE_DK").isNotNull())
        pid_race_dk_map_df = patient_race_df.select("NFER_PID", "SYN_RACE_DK").distinct()
        dim_race_df = patient_race_df.select("SYN_RACE_DK", "RACE_NAME").distinct()

        nfer_schema = self.read_datagen_schema("patient")
        patient_df = self.read_versioned_datagen_dir("PATIENT")
        patient_df = patient_df.select(nfer_schema)
        patient_df = patient_df.join(F.broadcast(pid_race_dk_map_df), ["NFER_PID"], "left")
        out_dir = os.path.join(self.DIM_SYN_INTERIM_DIR, "DIM_SYN_RACE")
        self.write_df_to_delta_lake(dim_race_df, out_dir)

        #TODO Versioning!
        pid_race_dk_map_df = pid_race_dk_map_df.withColumn("VERSION", F.lit(self.RUN_VERSION))
        pid_race_dk_map_df = pid_race_dk_map_df.withColumn("UPDATED_BY", F.lit("0.0"))

        out_dir = os.path.join(self.DATA_DIR, "DELTA_TABLES", "SYN_TABLES", "FACT_SYN_PATIENT_RACE_DK_MAP.parquet")
        self.write_df_to_delta_lake(pid_race_dk_map_df, out_dir)

        # patient_df = patient_df.cache()
        # out_dir_new = os.path.join(self.DATA_DIR, "DATAGEN.parquet", "PATIENT_NEW")
        # self.df_to_parquet(patient_df, out_dir_new)

        # out_dir = os.path.join(self.DATA_DIR, "DATAGEN.parquet", "PATIENT")
        # patient_df = self.read_parquet_to_df(out_dir_new)
        # self.df_to_parquet(patient_df, out_dir)
        # self.rmtree(out_dir_new)



if __name__ == "__main__":
    PatientRaceMerger().merge_race_data()
