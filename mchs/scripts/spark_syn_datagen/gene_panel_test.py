import os
import time
from pyspark.sql import *
from pyspark.sql import functions as F
from spark_jobs.orchestrator import Orchestrator
from core.config_vars import RunMode
import functools


# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


ONE_GB = 1024 * 1024 * 1024


class GenomicData(Orchestrator):

    def __init__(self, debug=False, demo=False):
        super().__init__()
        self.FLOWSHEET_COL_MAP = None
        self.LABTEST_COL_MAP = None
        self.parent = self
        self.debug = debug
        self.demo = demo
        self.RUN_MODE = RunMode.Full.value
        self.all_config_dict = self.SYSTEM_DICT["gene_panel_tests"]

    def generate_sparse_mutations_data(self):
        """
        index,NFER_PID,NFER_DTM,GENE,STATUS
        0,6100130,1552573260,AKT1,NEGATIVE
        1,6100130,1552573260,CDKN2B,NEGATIVE
        2,6100130,1552573260,GLI2,NEGATIVE
        3,6100130,1552573260,KLF4,NEGATIVE
        """
        # READ DATA
        start = time.perf_counter()
        self.init_spark_session()
        config_dict = self.all_config_dict['sparse_mutations']
        read_dir_path = config_dict['absolute_source_dir']  # absolute path
        nfer_schema = config_dict['nfer_schema']

        # the files are organized in csv files under each folder named with test ID
        subdir_names = [name for name in os.listdir(read_dir_path) if os.path.isdir(os.path.join(read_dir_path, name))]
        dflst = []
        count_sum = 0
        for test_id in subdir_names:
            df = self.read_csv_to_df(os.path.join(read_dir_path, test_id), sep=',')
            df = df.withColumn("TEST_ID", F.lit(test_id))
            cnt = df.count()
            count_sum+= cnt
            if self.debug:
                df.show()
                print(f'Number of rows post reading: {cnt}')
            dflst.append(df)

        df = unionAll(dflst)
        assert (count_sum==df.count())
        if self.debug:
            print(f'Number of rows post union: {df.count()}')

        df = df.distinct()
        if self.debug:
            print(f'No of rows post selecting distinct {df.count()}')
        df = df.na.drop("any", subset=["NFER_PID", "NFER_DTM"])
        if self.debug:
            print(f'Number of rows post removing null: {df.count()}')

        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT", ["NFER_PID", "BIRTH_DATE"]).distinct()
        df = df.join(F.broadcast(dim_patient_df), ["NFER_PID"])
        df = self.calc_nfer_age_for_nfer_dtm(df)
        df = self.assign_file_id_for_nfer_pid(df, "DIM_PATIENT")
        df = self.generate_row_id(df)
        df = df.withColumn("VERSION", F.lit(self.RUN_VERSION))
        df = df.withColumn("UPDATED_BY", F.lit(0.0))

        if self.debug:
            print(f'Final Data size: {df.count()}')

        # HYC writing
        df.show()
        out_dir = os.path.join(self.DATA_DIR, config_dict["dir_name"])
        print("writing hyc files to ", out_dir)
        self.df_to_patient_blocks(df, out_dir, nfer_schema)

        out_dir = f'/data/SPARK_SQL_DELTA_TABLES/SPARK_SQL_{self.RUN_VERSION}_{time.strftime("%d%m%Y")}'
        delta_table_path = os.path.join(out_dir,
                                        f"{config_dict['dir_name']}.parquet")
        print(f'Writing parquet files to to {delta_table_path}')
        df = df.select(nfer_schema)
        print('Schema for parquet')
        df.printSchema()
        print("Size of data: ", df.count())
        self.write_df_to_delta_lake(df, delta_table_path)
        end = time.perf_counter()
        print("Genetic data HYC and parquet : Total Time =", end - start)

    def generate_genomic_data(self):
        """
        TYPE,GENE,DNA CHANGE,NFER_PID,NFER_DTM
        VUS,COL6A3,C.6868C>T,6100130,1552573260
        VUS,HDAC9,C.2413G>A,6100130,1552573260
        VUS,HDAC9,C.2840C>G,6100130,1552573260
        """
        # READ DATA
        start = time.perf_counter()
        spark: SparkSession = self.init_spark_session()
        config_dict = self.all_config_dict['sparse_mutations']
        read_dir_path = config_dict['absolute_source_dir']  # absolute path
        nfer_schema = config_dict['nfer_schema']

        # the files are organized in csv files under each folder named with test ID
        # the files are organized in csv files under each folder named with test ID
        subdir_names = [name for name in os.listdir(read_dir_path) if os.path.isdir(os.path.join(read_dir_path, name))]
        dflst = []
        count_sum = 0
        for test_id in subdir_names:
            df = self.read_csv_to_df(os.path.join(read_dir_path, test_id), sep=',')
            df = df.withColumn("TEST_ID", F.lit(test_id))
            cnt = df.count()
            count_sum += cnt
            if self.debug:
                df.show()
                print(f'Number of rows post reading: {cnt}')
            dflst.append(df)

        df = unionAll(dflst)
        assert (count_sum == df.count())
        if self.debug:
            print(f'Number of rows post union: {df.count()}')

        df = df.distinct()

        if self.debug:
            print(f'No of rows post selecting distint {df.count()}')
        df = df.withColumnRenamed("DNA CHANGE", "DNA_CHANGE")
        df = df.na.drop("any", subset=["NFER_PID", "NFER_DTM"])
        if self.debug:
            print(f'Number of rows post removing null: {df.count()}')

        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT", ["NFER_PID", "BIRTH_DATE", "PATIENT_MERGED_FLAG"])
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)
        df = df.join(F.broadcast(dim_patient_df), ["NFER_PID"])
        df = self.calc_nfer_age_for_nfer_dtm(df)
        df = self.assign_file_id_for_nfer_pid(df, "DIM_PATIENT")
        df = self.generate_row_id(df)
        df = df.withColumn("VERSION", F.lit(self.RUN_VERSION))
        df = df.withColumn("UPDATED_BY", F.lit(0.0))

        if self.debug:
            print(f'Final Data size: {df.count()}')

        # WRITE DATA
        out_dir = os.path.join(self.DATA_DIR, config_dict["dir_name"])
        print("writing to ", out_dir)

        # HYC writing
        self.df_to_patient_blocks(df, out_dir, nfer_schema)

        out_dir = f'/data/SPARK_SQL_DELTA_TABLES/SPARK_SQL_{self.RUN_VERSION}_{time.strftime("%d%m%Y")}/'
        delta_table_path = os.path.join(out_dir,
                                        f"{config_dict['dir_name']}.parquet")
        print(f'Writing parquet files to to {delta_table_path}')
        df = df.select(nfer_schema )
        df.printSchema()
        self.write_df_to_delta_lake(df, delta_table_path)
        end = time.perf_counter()
        print("Genetic data HYC and parquet : Total Time =", end - start)

    def run(self):
        if 'gene_panel_test' in self.options.run:
            self.generate_genomic_data()
        if 'sparse_mutations' in self.options.run:
            self.generate_sparse_mutations_data()


if __name__ == '__main__':
    gd = GenomicData(debug=True).run()
