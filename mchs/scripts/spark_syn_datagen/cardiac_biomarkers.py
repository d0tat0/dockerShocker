import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from core.core_base import CoreBase
from spark_cohort_datagen.table_db import TableDB

CSV_PATH = "gs://cdap-data-dev/RESOURCES/5.031/FACT_SYN_CARDIAC_BIOMARKERS/content.csv"
WRITE_PATH = "gs://cdap-data-dev/USERS/rahul/ORCHESTRATOR/"
TABLE_NAME = "FACT_SYN_CARDIAC_BIOMARKERS"


class CardiacBiomarkerDatagen:
    def __init__(self, raw_data_path, write_base_path, version, table_schema):
        self._raw_data_path = raw_data_path
        self._write_base_path = write_base_path
        self._version = version
        self._table_schema = table_schema
        self._write_path = (
            os.path.join(
                write_base_path, self._version, "DELTA_TABLES", "SYN_TABLES", TABLE_NAME
            )
            + ".parquet"
        )

    def _infer_columns_from_path(self, df, spark):
        def get_study_id(path):
            return path.split("/")[5]

        def get_series_id(path):
            sid = path.split("/")[6]
            return sid[:-7]

        df = df.drop("STUDY_INSTANCE_UID")
        df = df.drop("SERIES_INSTANCE_UID")
        pdf = df.toPandas()
        pdf["STUDY_INSTANCE_UID"] = pdf["path"].apply(get_study_id)
        pdf["SERIES_INSTANCE_UID"] = pdf["path"].apply(get_series_id)
        pdf = pdf.astype(
            {"STUDY_INSTANCE_UID": "string", "SERIES_INSTANCE_UID": "string"}
        )
        df = spark.createDataFrame(pdf)

        return df

    def reaname_columns(self, df):
        for col in df.columns:
            df = df.withColumnRenamed(col, col.upper())
        return df

    def add_version_and_updated_by_cols(self, df, version=5.029):
        version = round(float(version), 3)
        df = df.withColumn("VERSION", F.lit(version))
        df = df.withColumn("UPDATED_BY", F.lit(0.0))
        return df

    def enforce_schema(self, df):
        # change datatype of NFER_PID,NFER_DTM to integer
        df = df.withColumn("NFER_PID", F.col("NFER_PID").cast("int"))
        df = df.withColumn("NFER_DTM", F.col("NFER_DTM").cast("int"))
        df = df.withColumn("NFER_AGE", F.col("NFER_AGE").cast("int"))
        df = df.withColumn("PV_COUNT", F.col("PV_COUNT").cast("int"))

        for col in ["STUDY_INSTANCE_UID", "SERIES_INSTANCE_UID"]:
            df = df.withColumn(col, F.col(col).cast("string"))
        return df

    def reorder_cols(self, df):
        # put the columns NFER_PID,NFER_DTM at the beginning
        cols = df.columns
        cols.remove("NFER_PID")
        cols.remove("NFER_DTM")
        cols.remove("NFER_AGE")
        cols = ["NFER_PID", "NFER_DTM", "NFER_AGE"] + cols
        df = df.select(cols)
        return df

    def read_latest_delta_table(self, spark, table_name, version):
        table_name = table_name.upper()
        self.table_db = TableDB()
        path = self.table_db.get_delta_path_by_id(table_name, version)
        print(f"Reading from {path}")
        df = spark.read.parquet(path)
        df = df.filter(F.col("UPDATED_BY") == 0.0)
        return df

    def add_nfer_columns(self, df, spark):
        if (
            len(set(["NFER_PID", "NFER_DTM", "NFER_AGE"]).intersection(set(df.columns)))
            == 3
        ):
            return df
        df = df.drop(*["NFER_PID", "NFER_DTM", "NFER_AGE"])
        join_df = self.read_latest_delta_table(
            spark, "FACT_SYN_DICOM_RADIOLOGY_SUMMARY", self._version
        )
        join_df = join_df.select(
            [
                "NFER_PID",
                "NFER_DTM",
                "NFER_AGE",
                "STUDY_INSTANCE_UID",
                "SERIES_INSTANCE_UID",
            ]
        ).distinct()
        df = join_df.join(
            F.broadcast(df),
            on=["STUDY_INSTANCE_UID", "SERIES_INSTANCE_UID"],
        )
        return df

    def run(self):
        # init spark

        spark = SparkSession.builder.appName(
            "process_fact_syn_cardiac_biomarkers"
        ).getOrCreate()
        print(f"Reading from {self._raw_data_path}")
        csv_df = spark.read.csv(self._raw_data_path, header=True)
        df = csv_df
        pre_count = df.count()
        df = self._infer_columns_from_path(df, spark)
        df = self.reaname_columns(df)
        df = df.select(list(set(df.columns).intersection(set(self._table_schema))))
        df = self.add_nfer_columns(df, spark)
        df = self.enforce_schema(df)
        df = self.reorder_cols(df)
        df = self.add_version_and_updated_by_cols(df, self._version)
        df = df.select(self._table_schema)

        df = df.cache()
        # validation
        df.printSchema()
        df.show(1, vertical=True, truncate=False)
        if pre_count != df.count():
            print(f"Count mismatch after processing: {pre_count} vs {df.count()}")
            select_cols = ["STUDY_INSTANCE_UID", "SERIES_INSTANCE_UID"]
            csv_df.select(select_cols).join(
                df.select(select_cols), on=select_cols
            ).show(10, truncate=False)
        assert len(set(df.columns).difference(set(self._table_schema))) == 0
        # count nulls in nfer_pid,nfer_dtm,nfer_age
        for col in ["NFER_PID", "NFER_DTM", "NFER_AGE"]:
            print(f"Null count in {col} is {df.filter(F.col(col).isNull()).count()}")

        partition_by = ["VERSION", "UPDATED_BY"]
        print(f"Writing to {self._write_path}")

        df.write.partitionBy(*partition_by).format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(self._write_path)


if __name__ == "__main__":
    """
    parser = argparse.ArgumentParser(
        prog="Augmented curation and ENTITY pref name table",
        description="Process cardiac biomarkers data",
    )
    parser.add_argument("-r", "--raw-data-path", default=CSV_PATH)
    parser.add_argument("-w", "--write-base-path", default=WRITE_PATH)
    parser.add_argument("-v", "--version", default="5.031")
    args = parser.parse_args()
    """
    orch_config = CoreBase(None)
    orch_config.get_dirs()
    table_schema = orch_config.SYSTEM_DICT["syn_tables"][TABLE_NAME]["nfer_schema"]
    CardiacBiomarkerDatagen(
        CSV_PATH, orch_config.ROOT_DIR, orch_config.RUN_VERSION, table_schema
    ).run()
