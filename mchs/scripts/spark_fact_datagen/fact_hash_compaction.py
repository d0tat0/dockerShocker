import os
import datetime
import time
import shutil
from collections import Counter
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator


class FactHashCompactJob(Orchestrator):

    def __init__(self):
        super().__init__()
        self.parent = self


    def run(self):
        table_list = self.parse_options_source()
        if 'gen_fact_hash' in self.options.run:
            self.generate_fact_hash(table_list)
        if 'extract_fact_hash' in self.options.run:
            self.extract_fact_hash(table_list)
        if 'merge_fact_hash' in self.options.run:
            self.merge_and_validate_fact_hash()


    def generate_fact_hash(self, source_list):
        begin = time.perf_counter()
        self.init_spark_session(expand_partitions=True) # expand partition to 5000

        self.load_fact_hash_meta_dict() # source -> (source_id,{hash_field,field_id})
        filtered_source_list = [s for s in self.fact_hash_meta.keys() if s in source_list]

        # READ DATA n MAPS
        """
        #looks like
        +------+------+----------+--------------------+--------+
        |ROW_ID|SOURCE|HASH_FIELD|          HASH_VALUE|NFER_PID|
        +------+------+----------+--------------------+--------+
        |     1|     0|         0|aaaaaabbbbbbccccc...|01234567|
        |     1|     0|         1|dd1345243432saasd...|01234567
        |     1|     0|         2|fde12234542234aad...|01234567|
        |     2|     0|         0|fde12234542234aad...|01234567|
        |     2|     0|         1|fdea2234542234aad...|01234567|
        |     2|     0|         2|fde12a3454s2234aa...|01234567|
        |     3|     0|         0|fde1a534542234aad...| 1234567|
        |     3|     0|         1|fde122345a2234aad...| 1234567|
        |     3|     0|         2|fde122345422f34ad...| 1234567|
        ...
        ...
        
        one row for each of the hash values explosion still small for delta
        """
        fact_hash_df = self.read_source_hash_fields(filtered_source_list)

        # # NFER_PID,HASH_ID,HASH_VALUE,... and    NFER_PID,MAX_HASH_ID
        # ~2tb in size not partitioned
        # parquet size is ~400-500 mb upto 1 gb.. not suitable for small execs
        hash_maps_df, max_hash_id_df = self.load_hash_maps_df()
        hash_maps_df = hash_maps_df.drop_duplicates(["NFER_PID", "HASH_VALUE"])

        # JOIN HASH MAPS
        fact_hash_df = fact_hash_df.join(hash_maps_df, ["NFER_PID", "HASH_VALUE"], "LEFT")
        fact_hash_df = fact_hash_df.join(F.broadcast(max_hash_id_df), ["NFER_PID"], "LEFT")
        fact_hash_df = fact_hash_df.na.fill(-1, ["HASH_ID"]).na.fill(0, ["MAX_HASH_ID"])

        # GENERATE NEW HASH_ID
        hashWindow = Window.partitionBy("NFER_PID").orderBy("HASH_ID", "HASH_VALUE")
        fact_hash_df = fact_hash_df.withColumn("HASH_ID",
            F.when(F.col("HASH_ID").isin(-1), F.col("MAX_HASH_ID") + F.dense_rank().over(hashWindow))
            .otherwise(F.col("HASH_ID")))

        # WRITE INTERIM HASH MAPS
        out_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "INTERIM_FACT_HASH")
        self.df_to_parquet(fact_hash_df, out_dir)
        end = time.perf_counter()
        print("FACT HASH: Total Time =", end - begin)


    def extract_fact_hash(self, source_list):
        begin = time.perf_counter()
        self.init_spark_session(expand_partitions=True)

        # READ INTERIM HASH MAPS
        self.load_fact_hash_meta_dict()
        filtered_source_list = [s for s in self.fact_hash_meta.keys() if s in source_list]
        interim_fact_hash_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "INTERIM_FACT_HASH")
        interim_fact_hash_df = self.read_parquet_to_df(interim_fact_hash_dir)
        interim_fact_hash_df.cache()

        # EXTRACT HASH & HASH_MAPS
        delta_hash_maps_df = interim_fact_hash_df.filter(F.col("HASH_ID") > F.col("MAX_HASH_ID"))
        self.write_delta_hash_maps(delta_hash_maps_df)
        self.write_fact_hash(filtered_source_list, interim_fact_hash_df)

        # COUNTERS
        counter = Counter()
        counter.update({"old_hash_mapped_values": interim_fact_hash_df.filter(F.col("HASH_ID") < F.col("MAX_HASH_ID")).count()})
        counter.update({"new_hash_mapped_values": delta_hash_maps_df.count()})
        counter.update({"unmapped_hash_values": interim_fact_hash_df.filter(F.col("HASH_ID").isin(-1)).count()})

        end = time.perf_counter()
        counter.setdefault("run_time", end - begin)
        self.log_spark_stats("HASH", "FACT_HASH", counter)
        print("FACT HASH: Total Time =", end - begin)

    def merge_and_validate_fact_hash(self):
        start = time.perf_counter()
        job_specific_partitions = 20000
        self.init_spark_session(expand_partitions=True,num_partitions = job_specific_partitions)
        hash_maps_df, _ = self.load_hash_maps_df()
        hash_maps_df.explain()
        hash_maps_df = hash_maps_df.select(["NFER_PID", "HASH_ID", "HASH_VALUE"])
        hash_maps_df.repartition(20000,"NFER_PID", "HASH_ID", "HASH_VALUE")
        print(hash_maps_df.count())
        hash_maps_df.explain()
        delta_hash_map_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "DELTA_FACT_HASH")
        if not self.glob(delta_hash_map_dir):
            print("No delta hash maps to merge")
            return

        delta_hash_maps_df = self.read_parquet_to_df(delta_hash_map_dir)
        delta_hash_maps_df = delta_hash_maps_df.select(["NFER_PID", "HASH_ID", "HASH_VALUE"])
        hash_maps_df = hash_maps_df.union(delta_hash_maps_df)
        hash_maps_df.explain()

        window = Window.partitionBy("NFER_PID").orderBy("HASH_ID", "HASH_VALUE")
        hash_maps_df = hash_maps_df.withColumn("KEY_HASH", F.concat("HASH_ID", "HASH_VALUE"))
        hash_maps_df = hash_maps_df.withColumn("DUPLICATE", F.lag("KEY_HASH", 1, "").over(window) == F.col("KEY_HASH"))
        hash_maps_df = hash_maps_df.filter(F.col("DUPLICATE") == False)
        hash_maps_df = hash_maps_df.drop("DUPLICATE").drop("KEY_HASH").cache()
        hash_maps_df.explain()
        print(hash_maps_df.count())
        #hash_maps_df = hash_maps_df.select(["NFER_PID", "HASH_ID", "HASH_VALUE"]).distinct().cache()

        stats_df = hash_maps_df.groupBy("NFER_PID").agg(
            F.count("*").alias("COUNT"),
            F.countDistinct("HASH_ID").alias("HASH_ID_COUNT"),
            F.countDistinct("HASH_VALUE").alias("HASH_VALUE_COUNT")
        )
        invalid_df = stats_df.filter(
            (F.col("COUNT") != F.col("HASH_ID_COUNT")) | (F.col("COUNT") != F.col("HASH_VALUE_COUNT"))
        )
        invalid_df.cache()
        invalid_count = invalid_df.count()
        print("Invalid PID Count =", invalid_count)
        if invalid_count > 0:
            invalid_df.show(20, False)
            raise Exception("Hash Map Validation Failed for {} PIDs".format(invalid_count))

        print("Hash Map Validation Passed, updating HASH_MAPS")
        hash_maps_df.explain()
        self.update_fact_hash(hash_maps_df)
        self.cleanup_interim_data()
        print("Total Time =", time.perf_counter() - start)

    def load_hash_maps_df(self,compute_max_hash_id=True):
        """
        +--------+-------+--------------------+
        |NFER_PID|HASH_ID|          HASH_VALUE|
        +--------+-------+--------------------+
        |    9599|     54|439721006a927ab4f...|
        |    9599|    860|fa59f95e8bad911e8...|
        |    9599|    861|049323cd0636ed590...|
        +--------+-------+--------------------+

        """
        hash_map_dir = self.latest_data_dir("HASH_MAPS", "FACT_HASH")
        if (not hash_map_dir or self.RUN_MODE == RunMode.Full.value) and self.OLD_MAPS_DIR:
            hash_map_dir = os.path.join(self.OLD_MAPS_DIR, "HASH_MAPS", "FACT_HASH")
        if hash_map_dir and self.glob(hash_map_dir):
            hash_maps_df = self.read_parquet_to_df(hash_map_dir)
        else:
            nfer_schema = ["NFER_PID", "HASH_ID", "HASH_VALUE"]
            type_schema = ["INTEGER", "INTEGER", "STRING"]
            hash_maps_df = self.create_empty_dataframe(nfer_schema, type_schema)
        if not compute_max_hash_id:
            max_hash_id_df = None
        else:
            max_hash_id_df = hash_maps_df.groupBy("NFER_PID").agg(F.max("HASH_ID").alias("MAX_HASH_ID"))

        return hash_maps_df, max_hash_id_df

    def update_fact_hash(self, hash_maps_df:DataFrame):
        hash_map_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "FACT_HASH")
        if self.glob(hash_map_dir):
            hash_map_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "FACT_HASH_PLUS")
            if self.glob(hash_map_dir):
                raise Exception("FACT_HASH_PLUS already exists, please merge FACT_HASH and FACT_HASH_PLUS")
        self.df_to_parquet(hash_maps_df, hash_map_dir)


    def cleanup_interim_data(self):
        delta_hash_map_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "DELTA_FACT_HASH")
        self.rmtree(delta_hash_map_dir)
        print("Deleted", delta_hash_map_dir)
        interim_fact_hash_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "INTERIM_FACT_HASH")
        self.rmtree(interim_fact_hash_dir)
        print("Deleted", interim_fact_hash_dir)
        max_hash_id_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "MAX_HASH_ID")
        self.rmtree(max_hash_id_dir)
        print("Deleted", max_hash_id_dir)
        delta_plus_fact_hash_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "DELTA_PLUS_FACT_HASH")
        self.rmtree(delta_plus_fact_hash_dir)
        print("Deleted", delta_plus_fact_hash_dir)


    def load_fact_hash_meta_dict(self):
        self.fact_hash_meta = {}
        fact_hash_dict = self.SYSTEM_DICT["hash_tables"]["nfer_hash"]
        source_list = []
        for source in fact_hash_dict:
            if source in self.SYSTEM_DICT["data_tables"]:
                source_list.append(source)
        for si, source in enumerate(source_list):
            hash_fields = fact_hash_dict[source].get('fact_hash', [])
            if not hash_fields:
                continue
            source = source.upper()
            print(source, si)
            hash_field_id = {}
            for fi, hash_field in enumerate(sorted(hash_fields)):
                hash_field_id.setdefault(hash_field, fi)
            self.fact_hash_meta.setdefault(source, {
                "source_id": si,
                "hash_field_id": hash_field_id
            })

    def read_source_hash_fields(self, source_list) -> DataFrame:
        schema = StructType([
            StructField(self.PATIENT_DK, StringType(), True),
            StructField("ROW_ID", IntegerType(), True),
            StructField("SOURCE", IntegerType(), True),
            StructField("HASH_FIELD", IntegerType(), True),
            StructField("HASH_VALUE", StringType(), True)
        ])

        hash_field_df = self.SPARK_SESSION.createDataFrame([], schema)
        for source in source_list:
            #source -> (source_id,{hash_field,field_id})
            fact_hash_meta = self.fact_hash_meta[source]
            print(fact_hash_meta)
            # read preprocessed
            source_df = self.read_source_files(source, version=self.RUN_VERSION)
            if source_df is None:
                print("No data for", source)
                continue
            source_df = source_df.withColumn("SOURCE", F.lit(fact_hash_meta["source_id"]))
            hash_fields = fact_hash_meta["hash_field_id"]
            hash_fields_str = ",".join([f"'{fi}', {hf}" for hf, fi in hash_fields.items()])
            hash_expr = "stack({0}, {1}) as (HASH_FIELD, HASH_VALUE)".format(len(hash_fields), hash_fields_str)
            source_df = source_df.select([self.PATIENT_DK, "ROW_ID", "SOURCE", F.expr(hash_expr)])
            hash_field_df = hash_field_df.union(source_df)

        dim_maps_df = self.read_versioned_dim_maps(columns=[self.PATIENT_DK, "NFER_PID"])
        hash_field_df = hash_field_df.join(F.broadcast(dim_maps_df), [self.PATIENT_DK], "LEFT")
        hash_field_df = hash_field_df.na.fill(0, subset=["NFER_PID"]).drop(self.PATIENT_DK)
        return hash_field_df

    def write_fact_hash(self, source_list, final_df: DataFrame):
        for source in source_list:
            hash_fields = self.fact_hash_meta[source]["hash_field_id"]
            source_df = final_df.filter(F.col("SOURCE") == self.fact_hash_meta[source]["source_id"])
            source_df = source_df.select(["NFER_PID", "ROW_ID", "HASH_FIELD", "HASH_ID"])

            if len(hash_fields) > 1:
                source_df = source_df.repartition(self.num_files * 5, "NFER_PID", "ROW_ID")
                source_df = source_df.groupBy("NFER_PID", "ROW_ID").pivot("HASH_FIELD", hash_fields.values()).agg(F.first("HASH_ID"))
            else:
                source_df = source_df.withColumnRenamed("HASH_ID", str(list(hash_fields.values())[0]))

            nfer_schema = ["NFER_PID", "ROW_ID"]
            for hash_field, hash_field_id in self.fact_hash_meta[source]["hash_field_id"].items():
                source_df = source_df.withColumnRenamed(str(hash_field_id), hash_field)
                nfer_schema.append(hash_field)

            out_dir = os.path.join(self.DATA_DIR, "HASH", source)
            self.df_to_parquet(source_df, out_dir, nfer_schema)

    def write_delta_hash_maps(self, final_df:DataFrame):
        hash_map_schema = ["NFER_PID", "HASH_ID", "HASH_VALUE"]
        hash_map_dir = os.path.join(self.DATA_DIR, "HASH_MAPS", "DELTA_FACT_HASH")
        self.df_to_parquet(final_df, hash_map_dir, hash_map_schema)


if __name__ == '__main__':
    FactHashCompactJob().run()
