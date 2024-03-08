import os
from time import time
from pyspark.sql import functions as F
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


class Death(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def generate_death_summary(self, syn_config):

        self.init_spark_session()
        source_df = self.read_versioned_datagen_dir("DIM_PATIENT")

        # TRANSFORM
        source_df = source_df.filter((F.upper("PATIENT_DECEASED_FLAG") == F.lit("Y")) & (F.col("PATIENT_DEATH_DATE").isNotNull()))
        source_df = self.de_dupe_patient_meta(source_df)
        source_df = source_df.withColumn("NFER_DTM", F.col("PATIENT_DEATH_DATE"))
        source_df = source_df.withColumnRenamed("PATIENT_DECEASED_FLAG", "DECEASED_FLAG")
        source_df = self.calc_nfer_age_for_nfer_dtm(source_df)

        # COUNTERS
        extra_counters = {'total_deceased': F.col("DECEASED_FLAG") == F.lit("Y")}
        self.write_versioned_syn_data(syn_config, source_df, extra_counters)


if __name__ == '__main__':

    out_table = 'death_summary'

    start = time()
    obj = Death()
    syn_config = obj.SYSTEM_DICT["syn_tables"][out_table]
    obj.generate_death_summary(syn_config)
    print("{} compelte! : Total Time = {} Mins".format(out_table, round((time() - start)/60)))
