import os
from pyspark.sql import *
from pyspark.sql import functions as F

from spark_jobs.orchestrator import Orchestrator


PATH_TO_VERSION = {
    "gs://cdap-raw-deid-data/20221021/": "5.000",
    "gs://cdap-raw-deid-data/20230106/": "5.003",
    "gs://cdap-raw-deid-data/20230126/": "5.005",
    "gs://cdap-raw-deid-data/20230210/": "5.007",
    "gs://cdap-raw-deid-data/20230315/": "5.009",
    "gs://cdap-raw-deid-data/20230327/": "5.011",
    "gs://cdap-raw-deid-data/20230428/": "5.013",
    "gs://cdap-raw-deid-data/20230525/": "5.015",
    "gs://cdap-raw-deid-data/20230621/": "5.017",
    "gs://cdap-raw-deid-data/20230720-EHR/": "5.019",
    "gs://cdap-raw-deid-data/incremental/June_2023/": "5.021"
}


class DimHashJob(Orchestrator):

    def __init__(self):
        super().__init__()
        self.init_spark_session()

    def read_cumulative_bridge_table(self, table):
        if not self.SPARK_SESSION:
            self.SPARK_SESSION = self.init_spark_session()

        df = None
        for base_path, version in PATH_TO_VERSION.items():
            table_path = os.path.join(base_path, table)
            if not self.glob(table_path):
                continue
            raw_df = self.SPARK_SESSION.read.csv(
                table_path,
                header="true",
                sep="|",
                encoding="iso-8859-1",
                mode="FAILFAST",
                quote=""
            )
            raw_df = raw_df.withColumn("VERSION", F.lit(version))
            if not df:
                df = raw_df
            else:
                df = df.unionByName(raw_df)

        distinct_cols = list(set(df.columns) - {"VERSION"})
        df = df.dropDuplicates(distinct_cols)

        return df

    def assign_nfer_pid(self, source_df: DataFrame) -> DataFrame:
        patient_map_df = self.read_versioned_dim_maps(columns=[self.PATIENT_DK, "NFER_PID", "VERSION"], latest_only=False)
        source_df = source_df.join(F.broadcast(patient_map_df), [self.PATIENT_DK, "VERSION"], "LEFT")
        source_df = source_df.na.fill(0, ["NFER_PID"])
        return source_df

    def _substitute_hash_values(self, df, table):
        hashed_columns = self.SYSTEM_DICT['hash_tables']['hash_maps']['bridge_hash'][table.lower()]['fact_hash']
        assert (len(hashed_columns) > 0)
        for hash_col in hashed_columns:
            df = df.withColumn(hash_col, F.xxhash64(hash_col))
        # assert (old_len == df.count())
        return df

    def generate_bridge_tables(self):
        table_list = self.parse_options_source()
        out_dir = "gs://cdap-orchestrator-offline-releases/ORCHESTRATOR/5.021/DELTA_TABLES/FACT_TABLES"
        for table in table_list:
            if self.skip_if_exists and self.glob(os.path.join(out_dir, f"{table}.parquet")):
                continue
            df = self.read_cumulative_bridge_table(table)
            df = self.assign_nfer_pid(df)
            df = self._substitute_hash_values(df, table)
            df = df.withColumn("UPDATED_BY", F.lit(0.0))
            df = df.coalesce(5000)
            curr_schema = df.columns
            curr_schema.remove("PATIENT_DK")
            df = df.select(curr_schema)
            print(f"Writing table: {os.path.join(out_dir, table)}")
            self.write_df_to_delta_lake(df, os.path.join(out_dir, f"{table}.parquet"))

    def run(self):
        if 'generate_bridge_tables' in self.options.run:
            self.generate_bridge_tables()

    def rewrite_df(self):
        table_list = self.parse_options_source()
        in_dir = "gs://cdap-orchestrator-offline-releases/ORCHESTRATOR/BRIDGE_TABLES"
        out_dir = "gs://cdap-orchestrator-offline-releases/ORCHESTRATOR/5.021/DELTA_TABLES/FACT_TABLES"
        for table in table_list:
            df = self.SPARK_SESSION.read.parquet(os.path.join(in_dir, table))
            curr_schema = df.columns
            curr_schema.remove("PATIENT_DK")
            df = df.select()
            self.write_df_to_delta_lake(df, os.path.join(out_dir, f"{table}.parquet"))


if __name__ == '__main__':
    DimHashJob().run()
