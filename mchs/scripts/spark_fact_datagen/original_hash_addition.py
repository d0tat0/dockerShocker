import os
import random
from pyspark.sql import functions as F
from spark_jobs.orchestrator import Orchestrator

table_to_col = {"FACT_PATHOLOGY": "SPECIMEN_ACCESSION_NUMBER",
                "FACT_RADIOLOGY": "ACCESSION_NBR"}


class DummyData(Orchestrator):

    def __init__(self):
        super().__init__()
        self.init_spark_session()

    def run(self):
        table_list = self.parse_options_source()
        tables_to_take = {'FACT_PATHOLOGY', 'FACT_RADIOLOGY'}
        tables = list(set(table_list).intersection(tables_to_take))
        version_list = self.VERSION_LIST
        if self.options.run == 'add_OH':
            self.add_OH(tables, version_list)

    def read_preprocesssed(self, table, version_list):
        final_df = None
        for version in version_list:
            table_path = os.path.join(self.ROOT_SOURCE_DIR, version, table)
            if self.glob(table_path):
                table_df = self.read_parquet_to_df(table_path)
                df = table_df.select(table_to_col[table])
                df = df.withColumn("XXhash",
                                   F.when(F.col(table_to_col[table]).isNotNull(), F.xxhash64(table_to_col[table])).otherwise(F.col(table_to_col[table])))
                if final_df is None:
                    final_df = df
                else:
                    final_df = final_df.union(df)
        return final_df

    def add_OH(self, tables, version_list):
        for table in tables:
            table_path = f'{self.ROOT_DIR}{self.RUN_VERSION}/DELTA_TABLES/FACT_TABLES/{table}.parquet'
            org_df = self.read_parquet_to_df(table_path)
            if f'{table_to_col[table]}_HASH' in org_df.columns:
                continue
            backup_path = f'gs://cdap-orchestrator-offline-releases/ORCHESTRATOR/Backups/{table}'
            self.upload_dir(table_path, backup_path)
            delta_df = self.read_parquet_to_df(table_path)
            pre_processed_df = self.read_preprocesssed(table, version_list)
            pre_processed_df = pre_processed_df.withColumnRenamed(table_to_col[table],
                                                                  f'{table_to_col[table]}_HASH')
            pre_processed_df = pre_processed_df.distinct()
            final_df = pre_processed_df.join(delta_df, pre_processed_df['XXhash'] == delta_df[table_to_col[table]], 'right')
            if table == 'FACT_RADIOLOGY':
                final_df = final_df.withColumn("ACCESSION_NBR_HASH_DICOM", F.expr("substring(ACCESSION_NBR_HASH, -16)"))
            final_df = final_df.drop('XXhash')
            final_df = final_df.select(
                *[col for col in final_df.columns if col != 'ACCESSION_NBR_HASH'],
                'ACCESSION_NBR_HASH'
            )
            self.write_df_to_delta_lake(final_df, table_path)


if __name__ == '__main__':
    DummyData().run()
