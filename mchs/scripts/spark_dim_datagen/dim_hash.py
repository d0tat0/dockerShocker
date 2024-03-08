import os
from collections import Counter
from pyspark.sql import *
from pyspark.sql import functions as F

from spark_jobs.orchestrator import Orchestrator


class DimHashJob(Orchestrator):

    def __init__(self):
        super().__init__()

    def run(self):
        if 'dim_hash' in self.options.run:
            self.generate_dim_hash()

    def generate_dim_hash(self):
        counter = Counter()
        self.init_spark_session()
        for dim_hash, hash_config in self.SYSTEM_DICT["hash_tables"]["hash_maps"].items():
            if dim_hash in ['fact_hash', 'bridge_hash']:
                continue

            dim_source = hash_config["dim_source"]
            dk_field = hash_config["nfer_schema"][1]
            dk_hash_field = f"{dk_field}_HASH"
            dk_id_field = f"{dk_field}_ID"
            print(dim_hash, dim_source, dk_field)

            dim_df = self.read_dim_source(dim_source)
            if not dim_df:
                print(f"Skipping {dim_hash} as {dim_source} is not available")
                continue
            else:
                dim_df = dim_df.withColumnRenamed(dk_field, dk_hash_field).cache()

            schema = self.build_schema([dk_id_field, dk_hash_field], ["INTEGER", "STRING"])
            hash_map_df = self.read_last_dim_hash_maps(hash_config["dir_name"], schema=schema)
            if not hash_map_df:
                hash_map_df = self.create_empty_dataframe([dk_id_field, dk_hash_field], ["INTEGER", "STRING"])

            hash_map_df.cache()
            final_df = dim_df.join(hash_map_df, [dk_hash_field], "LEFT")
            final_df.cache()

            mapped_df = final_df.filter(F.col(dk_id_field).isNotNull())
            unmapped_df = final_df.filter(F.col(dk_id_field).isNull())
            unmapped_count = unmapped_df.count()

            counter.update({
                "total_dim_values": final_df.count(),
                "mapped_dim_values": mapped_df.count(),
                "unmapped_dim_values": unmapped_count
            })

            if unmapped_count > 0:
                # GENERATE NEW ID
                max_id = hash_map_df.groupBy().max(dk_id_field).first()[0] or 0
                print(f"Max ID: {max_id}")
                orderWindow = Window.partitionBy().orderBy(dk_hash_field)
                unmapped_df = unmapped_df.withColumn(dk_id_field, F.lit(max_id) + F.dense_rank().over(orderWindow))
                unmapped_df = unmapped_df.select(dk_id_field, dk_hash_field)
                hash_map_df = hash_map_df.select(dk_id_field, dk_hash_field).union(unmapped_df)
                hash_map_df.cache()

            self.validate_hash_map(hash_map_df, dk_id_field, dk_hash_field)
            hash_map_df = hash_map_df.coalesce(1)
            out_dir = os.path.join(self.DATA_DIR, hash_config["dir_name"])
            self.df_to_parquet(hash_map_df, out_dir)
            self.log_spark_stats("HASH", dim_hash.upper(), counter)


    def validate_hash_map(self, hash_map_df: DataFrame, id_field_name, hash_field_name):
        map_count = hash_map_df.count()
        id_count = hash_map_df.select(id_field_name).distinct().count()
        hash_count = hash_map_df.select(hash_field_name).distinct().count()
        if map_count != id_count or map_count != hash_count:
            raise ValueError(f"Hash map validation failed. {map_count} != {id_count} != {hash_count}")
        print(f"{hash_field_name} MAP Validation passed.")


if __name__ == '__main__':
    DimHashJob().run()