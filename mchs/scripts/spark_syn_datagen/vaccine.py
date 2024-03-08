import os
from time import time
from pyspark.sql import functions as F, Window
from pyspark.sql import *
from spark_syn_datagen.synthetic_summaries import SynDataGenJob
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from core.config_vars import DataEnv
from core.config_vars import VACCINE_CODE_NAME_MAP, VACCINE_CODE_NAME_MAP_IRB
from core.config_vars import VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION
from core.config_vars import VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION_IRB
from core.config_vars import DAYS_FOR_FULL_VACCINATION

class Vaccine(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def load_vaccine_meta_df(self) -> DataFrame:
        vaccine_schema = StructType([
            StructField("VACCINE_NAME_DK", IntegerType(), False),
            StructField("COVID_VACCINE_NAME", StringType(), False),
            StructField("FULL_DOSE_COUNT", IntegerType(), False)
        ])
        if self.DATA_ENV == DataEnv.MAYO_CDAP.value:
            vaccine_codes = VACCINE_CODE_NAME_MAP
            fully_vaccinated_dose = VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION
        else:
            vaccine_codes = VACCINE_CODE_NAME_MAP_IRB
            fully_vaccinated_dose = VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION_IRB
        vaccine_meta = [(int(v), vaccine_codes[v], fully_vaccinated_dose[v]) for v in vaccine_codes]
        vaccine_code_df = self.SPARK_SESSION.createDataFrame(vaccine_meta, schema=vaccine_schema)
        return vaccine_code_df

    def generate_vaccine_summary(self, syn_config):

        self.init_spark_session()
        source_df = self.read_versioned_datagen_dir("FACT_IMMUNIZATIONS")
        vaccine_meta_df = self.load_vaccine_meta_df()

        # ADD VACCINE META
        if "LOCATION_SITE_NAME" not in source_df.columns:
            source_df = source_df.withColumn("LOCATION_SITE_NAME", F.lit("Other"))
        source_df = source_df.select(["FILE_ID", "NFER_PID", "NFER_DTM", "NFER_AGE", "VACCINE_NAME_DK", "LOCATION_SITE_NAME"])
        source_df = source_df.join(F.broadcast(vaccine_meta_df), ["VACCINE_NAME_DK"])
        source_df = source_df.dropDuplicates(["NFER_PID", "NFER_DTM", "VACCINE_NAME_DK"])

        # TRANSFORM
        pid_window = Window.partitionBy("NFER_PID").orderBy("NFER_DTM")
        source_df = source_df.withColumn("DOSE_COUNT", F.row_number().over(pid_window))
        source_df = source_df.withColumn("LAST_DOSE_DELTA_DTM",
            F.when(F.col("DOSE_COUNT") > 1, (F.col("NFER_DTM") - F.lag("NFER_DTM", 1, 0).over(pid_window)).cast(LongType()))
            .otherwise(F.lit(0))
        )
        source_df = source_df.withColumn("FULLY_VACCINATED", F.lit("NO"))
        source_df = source_df.withColumn("FULLY_VACCINATED_DTM",
            F.when(F.col("DOSE_COUNT") == F.col("FULL_DOSE_COUNT"), (F.col("NFER_DTM") + F.lit(DAYS_FOR_FULL_VACCINATION * 24 * 60 * 60)).cast(LongType()))
            .otherwise(F.lit(None))
        )
        source_df = source_df.withColumn("DOSAGE_COUNT", F.concat(F.lit("dosage_"), F.col("DOSE_COUNT")))
        source_df = source_df.withColumn("CUMULATIVE_VACCINE_DOSAGE_LIST", F.collect_list("COVID_VACCINE_NAME").over(pid_window))
        source_df = source_df.withColumn("CUMULATIVE_VACCINE_DOSAGE", F.concat_ws("_", F.col("CUMULATIVE_VACCINE_DOSAGE_LIST")))
        source_df = source_df.cache()

        # FULLY VACCINATED ROWS
        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT", ["NFER_PID", "BIRTH_DATE", "PATIENT_MERGED_FLAG"])
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)

        fully_vaccinated_df = source_df.filter(F.col("FULLY_VACCINATED_DTM").cast(LongType()) > 0)
        fully_vaccinated_df = fully_vaccinated_df.withColumn("NFER_DTM", F.col("FULLY_VACCINATED_DTM"))
        fully_vaccinated_df = fully_vaccinated_df.join(F.broadcast(dim_patient_df), ["NFER_PID"])
        fully_vaccinated_df = self.calc_nfer_age_for_nfer_dtm(fully_vaccinated_df)
        fully_vaccinated_df = fully_vaccinated_df.withColumn("DOSAGE_COUNT", F.lit(None))
        fully_vaccinated_df = fully_vaccinated_df.withColumn("COVID_VACCINE_NAME", F.lit(None))
        fully_vaccinated_df = fully_vaccinated_df.withColumn("CUMULATIVE_VACCINE_DOSAGE", F.lit(None))
        fully_vaccinated_df = fully_vaccinated_df.withColumn("FULLY_VACCINATED_DTM", F.lit(None))
        fully_vaccinated_df = fully_vaccinated_df.withColumn("LAST_DOSE_DELTA_DTM", F.lit(None))
        fully_vaccinated_df = fully_vaccinated_df.withColumn("FULLY_VACCINATED", F.lit("YES"))
        fully_vaccinated_df = fully_vaccinated_df.select(source_df.columns)

        # MERGE and DE-DUPE
        final_df = source_df.union(fully_vaccinated_df)
        unique_window = Window.partitionBy("NFER_PID", "NFER_DTM", "COVID_VACCINE_NAME").orderBy("NFER_PID", "NFER_DTM", "COVID_VACCINE_NAME")
        final_df = final_df.withColumn("ROW_NUM", F.row_number().over(unique_window))
        final_df = final_df.filter(F.col("ROW_NUM") == 1)

        # WRITE VERSIONED DATA
        extra_counters = {'fully_vaccinated_patients': F.col("FULLY_VACCINATED") == F.lit("YES")}
        self.write_versioned_syn_data(syn_config, final_df, extra_counters)


if __name__ == '__main__':

    out_table = 'vaccine_summary'

    start = time()
    obj = Vaccine()
    syn_config = obj.SYSTEM_DICT["syn_tables"][out_table]
    obj.generate_vaccine_summary(syn_config)
    print("{} compelte! : Total Time = {} Mins".format(out_table, round((time() - start)/60)))
