import os
import time

from pyspark.sql import *
from pyspark.sql import functions as F
from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator


DATA_OVERRIDES = {
    "dim_diagnosis_code": {
        "schema": [
            "DIAGNOSIS_CODE_DK",
            "DIAGNOSIS_DESCRIPTION",
            "DIAGNOSIS_CODE"
        ],
        "data": [
            ("46168909", "POST COVID-19 CONDITION, UNSPECIFIED", "U09.9"),
            ("46171929", "POST COVID-19 CONDITION, UNSPECIFIED", "U09.9"),
            ("45819904", "POST COVID-19 CONDITION, UNSPECIFIED", "U09.9")
        ]
    }
}


class DimInfoJob(Orchestrator):

    def __init__(self):
        super().__init__()

    def run(self):
        source_list = self.parse_options_dim_source()
        if 'pre_process' in self.options.run:
            self.pre_process_dim_data()
        if 'dim_version' in self.options.run:
            self.version_dim_data(source_list)
        if 'delta_table' in self.options.run:
            source_list = self.parse_options_dim_source()
            self.gen_spark_sql_delta_tables(source_list)

    def pre_process_dim_data(self):
        begin = time.perf_counter()
        self.init_spark_session()
        for table, dim_config in self.SYSTEM_DICT["info_tables"].items():
            dim_dir_name = os.path.splitext(dim_config["files"][0])[0]
            dim_file_path = os.path.join(self.INPUT_SOURCE_DIR, "DIM_INFO", dim_dir_name)
            if self.skip_if_exists and self.glob(dim_file_path):
                print(f"Skipping {dim_file_path} as it already exists")
                continue

            print(f'TABLE: {table}')
            table_schema = dim_config["amc_schema"]
            table_schema = [c for c in table_schema if c not in ["DEID_DTM", "DEID_VERSION", "EXTRACT_SOURCE"]]
            unique_indices = dim_config["unique_indices"]
            dim_df = self.raw_data_reader.read_raw_dim_source(table)
            if not dim_df or dim_df.count() == 0:
                print(f"Skipping {table} as it has no data")
                continue

            dim_df = dim_df.select(table_schema)
            if unique_indices:
                dim_df = dim_df.drop_duplicates(unique_indices)
            dim_df = dim_df.coalesce(1)
            self.df_to_parquet(dim_df, dim_file_path)
        end = time.perf_counter()
        print("PRE_PROCESSING DIM : Total Time = {}".format(end - begin))

    def version_dim_data(self, source_list):
        begin = time.perf_counter()
        self.init_spark_session()
        dim_table_config = self.SYSTEM_DICT["info_tables"]
        stats = {}
        for source in source_list:
            table = source.lower()
            file_name = dim_table_config[table]['files'][0]
            dim_dir_name = os.path.splitext(file_name)[0]
            dim_file_path = os.path.join(self.INPUT_SOURCE_DIR, "DIM_INFO", dim_dir_name)
            if not self.glob(dim_file_path):
                print(f"Skipping {dim_file_path} as it does not exist")
                continue

            dim_file_path = os.path.join(self.DATAGEN_DIR, "DIM_INFO", file_name)
            if self.skip_if_exists and self.glob(dim_file_path):
                print(f"Skipping {dim_file_path} as it already exists")
                continue

            # nfer schema is not added for mayo dim tables
            # TODO add nfer_schema for dim tables and read nfer_schema always
            if len(dim_table_config[table]["nfer_schema"]):
                table_schema = dim_table_config[table]["nfer_schema"]
            else:
                table_schema = dim_table_config[table]["amc_schema"]
            primary_key = dim_table_config[table]["unique_indices"]
            print(f"TABLE: {table}", "PRIMARY KEY: ", primary_key)
            table_schema = [c.upper() for c in table_schema if c not in ["DEID_DTM", "DEID_VERSION", "EXTRACT_SOURCE"]]

            new_dim_df = self.read_dim_source(table)
            if "dk_hash" in dim_table_config[table] and dim_table_config[table]["dk_hash"]:
                new_dim_df = self.replace_dim_hash(new_dim_df, table)

            if table in DATA_OVERRIDES:
                new_dim_df = self.apply_data_overrides(table, primary_key, new_dim_df)

            old_dim_path = self.last_data_dir("DATAGEN", "DIM_INFO", file_name)
            if self.RUN_MODE == RunMode.Full.value or not primary_key or not old_dim_path:
                new_dim_df = new_dim_df.withColumn("VERSION", F.lit(float(self.SOURCE_DATA_VERSION)))
                new_dim_df = new_dim_df.withColumn("UPDATED_BY", F.lit(0.0))
                new_dim_df.cache()
                self.write_dim_final(new_dim_df, table)
                stats[table] = {"inserts": new_dim_df.count()}
                new_dim_df.unpersist()
                continue

            if self.IS_CSV_MODE:
                old_dim_df = self.read_csv_to_df(old_dim_path)
            else:
                old_dim_df = self.read_parquet_to_df(old_dim_path)

            old_dim_df = old_dim_df.cache()
            # Do versioning only on the latest rows
            # For the rest of the rows which are add add them as is
            # 1| ---| --| 4.000| 4.001
            # 2| ---| --| 4.001|0.0
            # Keep 1 as is and use 2 for versioning with next versions
            old_dim_df_old = old_dim_df.filter(F.col("UPDATED_BY") != 0.0)
            old_dim_df = old_dim_df.filter(F.col("UPDATED_BY") == 0.0)
            old_dim_df = old_dim_df.withColumn("ROW_HASH",  F.md5(F.concat_ws("|", *table_schema)))
            for column in old_dim_df.columns:
                if column not in primary_key:
                    old_dim_df = old_dim_df.withColumnRenamed(column, 'OLD_' + column)

            new_dim_df = new_dim_df.withColumn("NEW_ROW_HASH",  F.md5(F.concat_ws("|", *table_schema)))
            # JOIN and find new and updates , add version for these, keep the older ones as same
            joined_dim = old_dim_df.join(new_dim_df, primary_key, "outer").cache()

            no_change_df = joined_dim.filter(F.col("NEW_ROW_HASH").isNotNull() & F.col("OLD_ROW_HASH").isNotNull() & (F.col("OLD_ROW_HASH") == F.col("NEW_ROW_HASH")))
            deletes_df = joined_dim.filter(F.col("NEW_ROW_HASH").isNull())
            inserts_df = joined_dim.filter(F.col("OLD_ROW_HASH").isNull())
            updates_df = joined_dim.filter(F.col("NEW_ROW_HASH").isNotNull() & F.col("OLD_ROW_HASH").isNotNull() & (F.col("OLD_ROW_HASH") != F.col("NEW_ROW_HASH")))
            stats[table] = {
                "no_change": no_change_df.count(),
                "deletes": deletes_df.count(),
                "inserts": inserts_df.count(),
                "updates": updates_df.count()
            }

            final_dim_df = no_change_df.select(*table_schema, "OLD_VERSION", "OLD_UPDATED_BY")

            # Even if a row in DIM DATA is deleted, do not consider it deleted as it may cause problems
            # for the Old FACT records referring to these DIM records, Commenting the below line
            # deletes_df = deletes_df.withColumn("OLD_UPDATED_BY", F.lit(self.RUN_VERSION))

            final_dim_df = final_dim_df.union(deletes_df.select(*table_schema, "OLD_VERSION", "OLD_UPDATED_BY"))
            final_dim_df = final_dim_df.drop("VERSION").drop("UPDATED_BY")
            final_dim_df = final_dim_df.withColumnRenamed("OLD_VERSION", "VERSION")
            final_dim_df = final_dim_df.withColumnRenamed("OLD_UPDATED_BY", "UPDATED_BY")
            inserts_df = inserts_df.select(*table_schema)
            inserts_df = inserts_df.withColumn("VERSION", F.lit(self.RUN_VERSION))
            inserts_df = inserts_df.withColumn("UPDATED_BY", F.lit("0.0"))
            old_df_columns = []
            for column in table_schema:
                if column not in primary_key:
                    old_df_columns.append("OLD_" + column)
                else:
                    old_df_columns.append(column)
            updates_df_old = updates_df.select(*old_df_columns, "OLD_VERSION")
            updates_df_old = updates_df_old.withColumnRenamed("OLD_VERSION", "VERSION")
            updates_df_old = updates_df_old.withColumn("UPDATED_BY", F.lit(self.RUN_VERSION))
            for column in updates_df_old.columns:
                if column.startswith("OLD_"):
                    updates_df_old = updates_df_old.withColumnRenamed(column, column.replace("OLD_", ""))

            final_dim_df = final_dim_df.union(updates_df_old)
            updates_new_df = updates_df.select(*table_schema)
            updates_new_df = updates_new_df.withColumn("VERSION", F.lit(self.RUN_VERSION))
            updates_new_df = updates_new_df.withColumn("UPDATED_BY", F.lit("0.0"))
            final_dim_df = final_dim_df.union(updates_new_df)

            inserts_df = inserts_df.withColumn("VERSION", F.lit(self.RUN_VERSION))
            inserts_df = inserts_df.withColumn("UPDATED_BY", F.lit("0.0"))

            final_dim_df = final_dim_df.union(inserts_df.select(*table_schema, "VERSION", "UPDATED_BY"))

            final_dim_df = final_dim_df.union(old_dim_df_old.select(*table_schema, "VERSION", "UPDATED_BY"))
            self.write_dim_final(final_dim_df, table)

        stats_file = os.path.join(self.DATA_DIR, "STATS", "dim_info_stats.json")
        self.dump_json_data(stats_file, stats)
        end = time.perf_counter()
        print("DIM VERSIONED: Total Time =", end - begin)

    def replace_dim_hash(self, dim_df: DataFrame, table):
        dim_hash_config = None
        
        if table.upper() in self.SYSTEM_DICT["hash_tables"].get("dim_xxhash", []):
            for field in self.SYSTEM_DICT["hash_tables"]["dim_xxhash"][table.upper()]:
                dim_df = dim_df.withColumn(field, F.xxhash64(F.col(field)))
            return dim_df

        dim_hash_maps = self.SYSTEM_DICT["hash_tables"]["hash_maps"]
        for config in dim_hash_maps.values():
            if config.get("dim_source").upper() == table.upper():
                dim_hash_config = config
                break
        if dim_hash_config is None:
            raise Exception(f"Hash config not found for table {table}")

        dk_field = dim_hash_config["nfer_schema"][1]
        hash_field = f"{dk_field}_HASH"
        id_field = f"{dk_field}_ID"
        hash_map_df = self.read_dim_hash_maps(dim_hash_config["dir_name"])
        dim_df = dim_df.withColumnRenamed(dk_field, hash_field)
        dim_df = dim_df.join(hash_map_df, hash_field).drop(hash_field)
        dim_df = dim_df.withColumnRenamed(id_field, dk_field)
        return dim_df

    def apply_data_overrides(self, table, primary_key, df: DataFrame) -> DataFrame:
        override_schema = DATA_OVERRIDES[table]["schema"]
        override_data = DATA_OVERRIDES[table]["data"]
        override_df = self.SPARK_SESSION.createDataFrame(override_data, override_schema)
        override_fields = [f for f in override_df.columns if f not in primary_key]
        for field in override_fields:
            override_df = override_df.withColumnRenamed(field, f"NEW_{field}")
        print(f"Applying data overrides for {table}")
        override_df.show(20, truncate=False)

        df = df.join(F.broadcast(override_df), primary_key, "left")
        for column in override_fields:
            df = df.withColumn(column,
                F.when(F.col(f"NEW_{column}").isNotNull(), F.col(f"NEW_{column}"))
                .otherwise(F.col(column))).drop(f"NEW_{column}")
        return df

    def gen_spark_sql_delta_tables(self, source_list):
        begin = time.perf_counter()
        print("Generating DIM Delta Tables for ", self.RUN_VERSION)
        for source in source_list:
            print(f'SOURCE: {source}')
            start = time.perf_counter()
            self.init_spark_session(source)
            dim_file_name, _ = os.path.splitext(self.SYSTEM_DICT["info_tables"][source.lower()]["files"][0])
            dim_df = self.read_versioned_dim_table(source, latest_only=False)
            if not dim_df:
                continue
            dim_df = dim_df.coalesce(1)
            self.release_versioned_delta_table(dim_df, dim_file_name, write_latest_delta=True)
            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
        end = time.perf_counter()
        print("SPARK_SQL: Total Time =", end - begin)


if __name__ == '__main__':
    DimInfoJob().run()
