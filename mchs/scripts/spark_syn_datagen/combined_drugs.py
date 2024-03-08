import os
from time import time
from pyspark.sql import functions as F, SparkSession
from spark_syn_datagen.synthetic_summaries import SynDataGenJob
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


class CombinedDrugs(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def generate_combined_drugs(self, syn_config):

        self.init_spark_session()
        source_list = syn_config["source_list"]

        # READ DATA
        schema = StructType([
            StructField("FILE_ID", LongType(), True),
            StructField("NFER_PID", LongType(), True),
            StructField("NFER_DTM", LongType(), True),
            StructField("NFER_AGE", LongType(), True),
            StructField("NFER_DRUG", StringType(), True),
            StructField("NFER_DRUG_CLASSES", StringType(), True)
        ])
        combined_df = self.SPARK_SESSION.createDataFrame([], schema)
        for source in source_list:
            source_df = self.read_versioned_datagen_dir(source)
            if source in self.BIG_TABLES:
                source_df = self.set_dim_file_id(source_df)
                source_df = source_df.drop("FILE_ID")
                source_df = source_df.withColumnRenamed("DIM_FILE_ID", "FILE_ID")
            if source == 'FACT_ORDERS':
                source_df = source_df.filter(F.col("ORDER_TYPE_DESCRIPTION").isin([
                    "Pharmacy",
                    "Inpatient Medication Order",
                    "Medications",
                    "Prescription Medication Order",
                    "Prescription Medication Order - Historical",
                    "Immunization/Injection",
                    "Immunization Medication Order",
                    "IV",
                    "Outpatient Medication Order",
                    "MEDICATION"
                ]))
            source_df = source_df.select("FILE_ID", "NFER_PID", "NFER_DTM", "NFER_AGE", "NFER_DRUG", "NFER_DRUG_CLASSES")
            source_df = source_df.filter(F.col("NFER_DRUG") != "")
            combined_df = combined_df.union(source_df)

        # TRANSFORM DATA
        combined_df_drug = combined_df.withColumn("DRUG_NAMES", F.split("NFER_DRUG", ","))
        combined_df_drug = combined_df_drug.withColumn("DRUG_NAMES", F.explode("DRUG_NAMES"))


        combined_df_drug = combined_df_drug.groupBy("NFER_PID", "NFER_DTM").agg(
            F.concat_ws(" nferdelimnfer ", F.collect_set("DRUG_NAMES")).alias("DRUG_NAMES"),
            F.first("FILE_ID").alias("FILE_ID"),
            F.first("NFER_AGE").alias("NFER_AGE")
        )

        combined_df_drug_classes = combined_df.withColumn("DRUG_CLASSES", F.split("NFER_DRUG_CLASSES", ","))
        combined_df_drug_classes = combined_df_drug_classes.withColumn("DRUG_CLASSES", F.explode("DRUG_CLASSES"))


        combined_df_drug_classes = combined_df_drug_classes.groupBy("NFER_PID", "NFER_DTM").agg(
            F.concat_ws(" nferdelimnfer ", F.collect_set("DRUG_CLASSES")).alias("DRUG_CLASSES")
        )

        combined_df_final = combined_df_drug.join(combined_df_drug_classes, ["NFER_PID", "NFER_DTM"], "left")
        combined_df_final = combined_df_final.select(combined_df_drug['*'], combined_df_drug_classes['DRUG_CLASSES'])

        # WRITE DATA
        self.write_versioned_syn_data(syn_config, combined_df_final)


if __name__ == '__main__':

    out_table = 'combined_drugs'

    start = time()
    obj = CombinedDrugs()
    syn_config = obj.SYSTEM_DICT["syn_tables"][out_table]
    obj.generate_combined_drugs(syn_config)
    print("{} compelte! : Total Time = {} Mins".format(out_table, round((time() - start)/60)))
