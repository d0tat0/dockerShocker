import os
import sys
import re
from datetime import datetime

from pyspark.sql import *
from pyspark.sql import functions as F

from core.config_vars import PATIENT_PER_FILE, DIR_DTM_FORMAT
from core.data_utils import UtilBase


class SparkJob(UtilBase):

    def __init__(self, config_file = None):
        super().__init__(config_file)
        self.SPARK_SESSION: SparkSession = None
        self.num_files = self.MAX_BUCKETS
        self.skip_if_exists = self.options.skip_if_exists.upper() == 'TRUE'
        if self.SAMPLE_RUN:
            self.skip_if_exists = False

    def init_spark_session(self, source=None, expand_partitions=False,num_partitions=None):
        module_name = os.path.basename(sys.argv[0]).split(".")[0]
        run = self.options.run if self.options.run else "job"
        self.SPARK_SESSION = SparkSession.builder.appName(f"{module_name}_{run}").getOrCreate()
        self.SPARK_SESSION.conf.set("spark.sql.files.maxPartitionBytes", "1gb")
        self.SPARK_SESSION.conf.set("spark.sql.broadcastTimeout", 60 * 60)
        self.SPARK_SESSION.sparkContext.setLogLevel("WARN")
        self.adjust_shuffle_partitions(source in self.BIG_TABLES or expand_partitions,num_partitions)
        return self.SPARK_SESSION

    def adjust_shuffle_partitions(self, expand_partitions,num_partitions=None):
        if expand_partitions:
            if num_partitions:
                self.SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", num_partitions)
            else:
                self.SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", self.num_files * 5)
        else:
            self.SPARK_SESSION.conf.set("spark.sql.shuffle.partitions", self.num_files)



    @staticmethod
    def join_with_null(
                       df1: DataFrame,
                       df2: DataFrame,
                       join_cols,
                       how='inner',
                       is_broadcast_df2=True):
        """

        """
        # make the join columns have uniform null/none/empty values
        replace_map = {float('nan'): None}
        replace_map2 = {'': None}
        df1 = df1.replace(to_replace=replace_map,
                          subset=join_cols)
        df2 = df2.replace(to_replace=replace_map,
                          subset=join_cols)

        df1 = df1.replace(to_replace=replace_map2,
                          subset=join_cols)
        df2 = df2.replace(to_replace=replace_map2,
                          subset=join_cols)

        # rename columns for df2 as these are added in the eqnullsafe join
        cond = []
        drop_cols = []
        for col in join_cols:
            df2 = df2.withColumnRenamed(col,"df2_"+col)
            drop_cols.append("df2_"+col)

        for col in join_cols:
            cond.append(df1[col].eqNullSafe(df2["df2_"+col]))

        if is_broadcast_df2:
            df1 = df1.join(F.broadcast(df2),
                       cond,
                       how)
        else:
            df1 = df1.join(df2,
                           cond,
                           how)
        df1 = df1.drop(*drop_cols)
        return df1

    def assign_file_id_for_nfer_pid(self, source_df: DataFrame, source, in_field = "NFER_PID", out_field = "FILE_ID"):
        if 'FILE_ID' in source_df.columns:
            return source_df

        source_df = source_df.withColumn("DIM_FILE_ID",
            ((F.col(in_field) % F.lit(self.MAX_BUCKETS)) * F.lit(PATIENT_PER_FILE) + F.lit(1)).cast("INT"))
        if source in self.BIG_TABLES:
            source_df = source_df.withColumn("FILE_INDEX", (F.col(in_field) / F.lit(self.MAX_BUCKETS)).cast("INT") % F.lit(5))
            source_df = source_df.withColumn("FILE_OFFSET", (F.col("FILE_INDEX") * F.lit(PATIENT_PER_FILE/5)).cast("INT"))
            source_df = source_df.withColumn(out_field, F.col("DIM_FILE_ID") + F.col("FILE_OFFSET"))
            source_df = source_df.drop("FILE_INDEX").drop("FILE_OFFSET")
        else:
            source_df = source_df.withColumn(out_field, F.col("DIM_FILE_ID"))
        return source_df

    def set_dim_file_id(self, source_df: DataFrame, field = "DIM_FILE_ID", src_field="FILE_ID") -> DataFrame:
        return source_df.withColumn(field,
            (F.col(src_field) / F.lit(PATIENT_PER_FILE)).cast("INT") * F.lit(PATIENT_PER_FILE) + F.lit(1)
        )

    def add_dtm_suffix(self, dir_path):
        if dir_path.startswith("gs://"):
            dtm_part = datetime.utcnow().strftime(DIR_DTM_FORMAT)
            dir_path = os.path.join(dir_path, dtm_part)
        return dir_path

    def latest_delta_dir(self, *dir_parts):
        p1 = str(os.path.join(self.ROOT_DIR, self.LATEST_DELTA_DIR, *dir_parts))
        p2 = str(os.path.join(self.WRITE_ROOT_DIR, self.LATEST_DELTA_DIR, *dir_parts))
        data_dir_path = p1 if self.glob(p1) else p2
        if not self.glob(data_dir_path):
            return self.latest_data_dir(*dir_parts)
        return data_dir_path

    def latest_data_dir(self, *dir_parts):
        data_dir, _ = self.scan_data_dir(*dir_parts)
        return data_dir

    def latest_delta_version(self, *dir_parts):
        p1 = str(os.path.join(self.ROOT_DIR, self.LATEST_DELTA_DIR, *dir_parts))
        p2 = str(os.path.join(self.WRITE_ROOT_DIR, self.LATEST_DELTA_DIR, *dir_parts))
        data_dir_path = p1 if self.glob(p1) else p2
        if not self.glob(data_dir_path):
            return self.latest_data_version(*dir_parts)
        max_version = self.SOURCE_DATA_VERSION
        paths = self.glob(os.path.join(data_dir_path,"*"))
        for path in paths:
            if "VERSION" not in path:
                continue
            match = re.search(r"/VERSION=(\d\.\d+)", path)
            if not match:
                continue
            _version = match.group(1)
            if float(_version) > float(max_version):
                max_version = _version
        return max_version

    def latest_data_version(self, *dir_parts):
        _, data_version = self.scan_data_dir(*dir_parts, log=False)
        return data_version

    def last_data_dir(self, *dir_parts, run_version=None):
        run_version = self.RUN_VERSION if run_version is None else run_version
        last_version = round(float(run_version) - 0.001, 3)
        data_dir, _ = self.scan_data_dir(*dir_parts, run_version=last_version)
        return data_dir

    def last_data_version(self, *dir_parts, run_version=None):
        run_version = self.RUN_VERSION if run_version is None else run_version
        last_version = round(float(run_version) - 0.001, 3)
        _, data_version = self.scan_data_dir(*dir_parts, run_version=last_version, log=False)
        return data_version

    def data_dir(self, dir_name, source, run_version=None):
        run_version = run_version or self.RUN_VERSION
        # check datagen_XX.parquet then datagen.parquet then datagen.final then datagen new parquet
        if self.glob(os.path.join(self.ROOT_DIR, run_version, f"{dir_name}_XX.parquet", source, "_SUCCESS")):
            return os.path.join(self.ROOT_DIR, run_version, f"{dir_name}_XX.parquet", source)
        if self.glob(os.path.join(self.WRITE_ROOT_DIR, run_version, f"{dir_name}_XX.parquet", source, "_SUCCESS")):
            return os.path.join(self.WRITE_ROOT_DIR, run_version, f"{dir_name}_XX.parquet", source)
        if self.glob(os.path.join(self.ROOT_DIR, run_version, f"{dir_name}.parquet", source, "_SUCCESS")):
            return os.path.join(self.ROOT_DIR, run_version, f"{dir_name}.parquet", source)
        if self.glob(os.path.join(self.WRITE_ROOT_DIR, run_version, f"{dir_name}.parquet", source, "_SUCCESS")):
            return os.path.join(self.WRITE_ROOT_DIR, run_version, f"{dir_name}.parquet", source)
        if self.glob(os.path.join(self.ROOT_DIR, run_version, dir_name, source, "header.csv")):
            return os.path.join(self.ROOT_DIR, run_version, dir_name, source)
        if self.glob(os.path.join(self.WRITE_ROOT_DIR, run_version, dir_name, source, "header.csv")):
            return os.path.join(self.WRITE_ROOT_DIR, run_version, dir_name, source)
        if self.glob(os.path.join(self.ROOT_DIR, run_version, dir_name, source, "_SUCCESS")):
            return os.path.join(self.ROOT_DIR, run_version, dir_name, source)
        if self.glob(os.path.join(self.WRITE_ROOT_DIR, run_version, dir_name, source, "_SUCCESS")):
            return os.path.join(self.WRITE_ROOT_DIR, run_version, dir_name, source)
        return None

