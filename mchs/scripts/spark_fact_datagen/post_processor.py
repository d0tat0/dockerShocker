import os
import random
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_jobs.orchestrator import Orchestrator


class PostProcessor(Orchestrator):

    def __init__(self):
        super().__init__()
        self.init_spark_session()

    def run(self):
        version = self.options.version or self.RUN_VERSION
        config = self.get_json_data(self.post_processor_config_file)
        tasks_dict = config.get('post_process', {})
        if not tasks_dict:
            print("Not running post processor... please check if you passed it in deploy_config or not."
                  " Or may be post_processor config is empty")
            return
        print(f'tasks_dict -> {tasks_dict}')
        for table, task_list in tasks_dict.items():
            out_dir = os.path.join(self.WRITE_ROOT_DIR, self.RUN_VERSION, 'DELTA_TABLES', 'FACT_TABLES', f'{table}.parquet')
            latest_delta_dir = None
            if version == self.RUN_VERSION:
                latest_delta_dir = os.path.join(self.WRITE_ROOT_DIR, self.LATEST_DELTA_DIR, 'DELTA_TABLES', 'FACT_TABLES', f'{table}.parquet')
            base_df = self.upload(table)
            for task in task_list:
                op = task['operation']
                print(f'Current operation -> {op} for table -> {table}')
                if op == 'add_OH':
                    self.add_OH(base_df, task, out_dir, latest_delta_dir)
                elif op == 'add_age':
                    self.add_age(base_df, task, out_dir, latest_delta_dir)
                elif op == 'add_harmonised_cols':
                    self.add_harmonised_cols(base_df, task, out_dir, latest_delta_dir)

    def upload(self, base_table):
        base_table_path = os.path.join(self.ROOT_DIR, self.RUN_VERSION, 'DELTA_TABLES', 'FACT_TABLES', f'{base_table}.parquet')
        print(f'base_table_path -> {base_table_path}')
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_root_path = os.path.join(self.ROOT_DIR, 'Backups')
        backup_table_path = os.path.join(backup_root_path, f'{base_table}.parquet')
        #if base_table == 'FACT_FLOWSHEETS':
            #path = 'gs://cdap-data-dev/ORCHESTRATOR/Backups/FACT_FLOWSHEETS.parquet_20240223050032'
            #base_df = self.read_delta_to_df(path)
        #elif base_table == 'FACT_LAB_TEST':
            #path = 'gs://cdap-data-dev/ORCHESTRATOR/Backups/FACT_LAB_TEST.parquet/'
            #base_df = self.read_delta_to_df(path)
        #else:
        if self.check_file_exists(backup_table_path):
            self.move_backup_table(backup_table_path)
        if self.check_file_exists(base_table_path):
            #self.rmtree(backup_table_path)
            self.backup_copy(base_table_path, backup_root_path)
            base_df = self.read_delta_to_df(backup_table_path)
        else:
            raise Exception(f'base_table_path {base_table_path} does not exist')
        return base_df

    def add_age(self, base_df, task, out_dir, latest_delta_dir=None):
        new_columns = task['new_columns']
        if all(column in base_df.columns for column in new_columns):
            print("All new columns already exist in the DataFrame.")
        else:
            reference_table = task['reference_table']
            reference_table_path = f'{self.ROOT_DIR}{self.RUN_VERSION}/DELTA_TABLES/FACT_TABLES/{reference_table}.parquet'
            reference_df = self.read_delta_to_df(reference_table_path)
            print(f'reference_df count -> {reference_df.count()}')
            reference_df = reference_df.filter(F.col("UPDATED_BY") == 0.0)
            reference_cols = task['refernce_columns']
            selected_cols = reference_cols + new_columns
            temp_reference_df = reference_df.select(*selected_cols)
            if task['order_in'] == 'asc':
                windowSpec = Window.partitionBy(*reference_cols).orderBy(F.col(task['order_by_column']).asc())
            else:
                windowSpec = Window.partitionBy(*reference_cols).orderBy(F.col(task['order_by_column']).dsc())
            temp_reference_df_with_row_number = temp_reference_df.withColumn('row_num', F.row_number().over(windowSpec))
            max_row_num_df = temp_reference_df_with_row_number.groupBy(*reference_cols).agg(
                F.max('row_num').alias('max_row_num'),
                *[F.last(col).alias(col) for col in new_columns]
            )
            last_rows_temp_df = max_row_num_df.drop('row_num')
            result_df = base_df.join(
                last_rows_temp_df.select(*selected_cols),
                reference_cols,
                'left'
            )
            if result_df.count() != base_df.count():
                raise Exception(
                    f'Count mismatch for the tables {result_df.count()} and {base_df.count()}')
            else:
                print("Counts matched")
                print(f"Columns of result_df -> {result_df.columns}")
                print(f"columns of base_df -> {base_df.columns}")
                self.write_df_to_delta_lake(result_df, out_dir)
                print('###############################################################################')

    def read_preprocesssed(self, table, version_list, run_version, base_col):
        final_df = None
        print(f'version_list -> {version_list}')
        for version in version_list:
            #if float(version) > float(run_version):
                #continue
            table_path = os.path.join(self.ROOT_SOURCE_DIR, version, table)
            if self.glob(table_path):
                table_df = self.read_parquet_to_df(table_path)
                df = table_df.select(base_col)
                df = df.withColumn("XXhash",
                                   F.when(F.col(base_col).isNotNull(),
                                          F.xxhash64(base_col)).otherwise(F.col(base_col)))
                if final_df is None:
                    final_df = df
                else:
                    final_df = final_df.union(df)
        return final_df

    def add_OH(self, delta_df, task_details, out_dir, latest_delta_dir=None):
        table = task_details['base_table']
        base_col = task_details['base_col']
        new_col = task_details['new_col']
        if new_col not in delta_df.columns:
            org_count = delta_df.count()
            pre_processed_df = self.read_preprocesssed(table, self.VERSION_LIST,  self.options.version, base_col)
            pre_processed_df = pre_processed_df.withColumnRenamed(base_col,
                                                                  f'{base_col}_HASH')
            pre_processed_df = pre_processed_df.distinct()
            final_df = pre_processed_df.join(delta_df, pre_processed_df['XXhash'] == delta_df[base_col], 'right')
            if table == 'FACT_RADIOLOGY':
                final_df = final_df.withColumn("ACCESSION_NBR_HASH_DICOM", F.expr("substring(ACCESSION_NBR_HASH, -16)"))
            final_df = final_df.drop('XXhash')
            columns = final_df.columns
            columns.remove(new_col)
            columns.append(new_col)
            final_df = final_df.select(*columns)
            final_count = final_df.count()
            if org_count == final_count:
                self.write_df_to_delta_lake(final_df, out_dir)
            else:
                print(f'counts mismatched')
                print(f'original count -> {org_count}')
                print(f'final count -> {final_count}')
        else:
            print(f'Original Hash col already present in table {table}')


    def add_harmonised_cols(self, base_df, task, out_dir, latest_delta_dir=None):
        base_df_count = base_df.count()
        table = task['base_table']
        print(f'base_df_count -> {base_df_count}')
        harmonized_table_subpath = task['harmonized_table_subpath']
        harmonized_output_path = os.path.join(self.ROOT_DIR, self.RUN_VERSION, harmonized_table_subpath)
        harmonized_df = self.read_delta_to_df(harmonized_output_path)
        if table == 'FACT_FLOWSHEETS':
            harmonized_df = harmonized_df.withColumnRenamed("SYN_DK", "SYN_FLOWSHEET_DK")
        if table == 'FACT_LAB_TEST':
            harmonized_df = harmonized_df.withColumnRenamed("SYN_DK", "SYN_LAB_TEST_DK")
        join_columns = task['join_cols']
        if 'add_on_cols' in task:
            add_on_cols = task['add_on_cols']
            cols_to_select = join_columns + add_on_cols
            harmonized_df = harmonized_df.select(*cols_to_select)
        result_df = self.join_with_null(base_df, harmonized_df, join_columns, "left")
        #result_df = self.add_syn_dk_col(result_df, task['base_table'])
        result_df_count = result_df.count()
        print(f'result_df_count {result_df_count}')
        if base_df_count == result_df_count:
            self.write_df_to_delta_lake(result_df, out_dir)
        else:
            raise Exception("counts mismatch")


if __name__ == '__main__':
    PostProcessor().run()
