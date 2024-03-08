import os

from spark_jobs.orchestrator import Orchestrator


class QuickDeltaFix(Orchestrator):
    def __init__(self):
        super().__init__()

    def fix_delta(self):
        spark = self.SPARK_SESSION
        delta_path = os.path.join(self.ROOT_DIR, str(self.RUN_VERSION), "DELTA_TABLES", "FACT_TABLES")
        fmv_path = os.path.join(self.ROOT_DIR, str(self.RUN_VERSION), "FACT_MAPS_VERSIONED.parquet")
        for _path in self.glob(delta_path):
            _path = _path.strip("/")
            table_name = _path.split("/")[-1].split(".")[0]
            if table_name == "DIM_PATIENT":
                continue
            df = spark.read.format("delta").load(_path)
            fmv= spark.read.parquet(os.path.join(fmv_path, table_name))
            df = df.drop("VERSION").drop("UPDATED_BY")
            df = df.join(fmv.select(["NFER_PID", "ROW_ID", "VERSION", "UPDATED_BY"]), ["NFER_PID", "ROW_ID"], "LEFT")
            write_path = os.path.join(self.ROOT_DIR, "LATEST_DELTA_TABLES", "DELTA_TABLES", "FACT_TABLES", f"{table_name}.parquet")
            self.write_df_to_delta_lake(df, write_path)
            self.overwrite_latest_delta(write_path, _path)

    def fix_dim_pat(self):
        spark = self.SPARK_SESSION
        delta_path = os.path.join(self.ROOT_DIR, str(self.RUN_VERSION), "DELTA_TABLES", "FACT_TABLES", "DIM_PATIENT.parquet")
        dmv_path = os.path.join(self.ROOT_DIR, str(self.RUN_VERSION), "DATAGEN.parquet", "DIM_MAPS_VERSIONED")
        df = spark.read.format("delta").load(delta_path)
        fmv= spark.read.parquet(dmv_path)
        df = df.drop("VERSION").drop("UPDATED_BY")
        df = df.join(fmv.select(["NFER_PID", "ROW_ID", "VERSION", "UPDATED_BY"]), ["NFER_PID", "ROW_ID"], "LEFT")
        write_path = os.path.join(self.ROOT_DIR, "LATEST_DELTA_TABLES", "DELTA_TABLES", "FACT_TABLES", "DIM_PATIENT.parquet")
        self.write_df_to_delta_lake(df, write_path)
        self.overwrite_latest_delta(write_path, delta_path)

    def run(self):
        if self.options.run == "fix_delta":
            self.fix_delta()
        if self.options.run == "fix_dim_pat":
            self.fix_dim_pat()