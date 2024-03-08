import os
from time import time
from pyspark.sql import functions as F, SparkSession, Window
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


LABTEST_HEADERS = ["NFER_PID", "NFER_DTM", "LAB_TEST_DK", "RESULT_TXT", "LOCATION_SITE_NAME"]
ADT_HEADERS = ["NFER_PID", "ADMIT_DTM", "DISCHARGE_DTM", "LOCATION_BED_TYPE", "PATIENT_PROCESS_TYPE"]
DIM_HEADERS = ["FILE_ID", "NFER_PID", "BIRTH_DATE", "PATIENT_DEATH_DATE", "PATIENT_DECEASED_FLAG", "PATIENT_MERGED_FLAG"]


class CovidFull(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def generate_covid_comprehensive_summary(self, syn_config):

        # READ COVID LAB TESTS
        covid_test_dk_file = self.latest_data_dir('DISEASE', 'COVID', 'fact_lab_test.dk')
        if not covid_test_dk_file:
            covid_test_dk_file = os.path.join(self.RESOURCE_OLD_MAPS_DIR, 'DISEASE', 'COVID', 'fact_lab_test.dk')
        covid_test_dks = self.open_file(covid_test_dk_file).readlines()
        print(f"COVID TEST DK FILE: {covid_test_dk_file} - {len(covid_test_dks)} covid lab test dks found.")

        spark: SparkSession = self.init_spark_session()
        covid_test_dks = [(int(dk.strip()),) for dk in covid_test_dks]
        covid_test_dk_df = spark.createDataFrame(covid_test_dks, ["LAB_TEST_DK"])
        fact_lab_test_df = self.read_versioned_datagen_dir("FACT_LAB_TEST", LABTEST_HEADERS)
        fact_lab_test_df = fact_lab_test_df.join(F.broadcast(covid_test_dk_df), ["LAB_TEST_DK"]).drop("LAB_TEST_DK")
        fact_lab_test_df = fact_lab_test_df.cache()

        # COVID TEST TIME
        covid_lab_data_df = fact_lab_test_df.filter(F.col("NFER_DTM").isNotNull())
        covid_lab_data_df = covid_lab_data_df.withColumn("COVID_POSITIVE_FLAG",
                                                         F.when(F.lower("RESULT_TXT").isin(
                                                             ["detected", "positive", "presumptive positive"]),
                                                                F.lit("Y")).otherwise(F.lit("N")))
        covid_lab_data_df.cache()

        # LOCATION SITE NAME
        covid_lab_data_df = self.get_normalized_site(covid_lab_data_df, "LOCATION_SITE_NAME")

        # READ HOSPITALIZATION DATA
        adt_df = self.read_versioned_datagen_dir("FACT_ADMIT_DISCHARGE_TRANSFER_LOCATION", ADT_HEADERS)

        adt_df = adt_df.where(adt_df.PATIENT_PROCESS_TYPE != 'Emergency').distinct()
        covid_patient_df = covid_lab_data_df.filter(F.col("COVID_POSITIVE_FLAG").isin("Y")).select("NFER_PID", "NFER_DTM")
        covid_adt_df = adt_df.join(F.broadcast(covid_patient_df), ["NFER_PID"])
        covid_adt_df = covid_adt_df.filter(F.col("ADMIT_DTM").isNotNull() & F.col("DISCHARGE_DTM").isNotNull())
        covid_adt_df = covid_adt_df.filter(F.col("ADMIT_DTM") < F.col("DISCHARGE_DTM"))
        covid_adt_df = covid_adt_df.filter(F.col("DISCHARGE_DTM") > F.col("NFER_DTM"))
        covid_adt_df = covid_adt_df.filter(F.col("ADMIT_DTM") < F.col("NFER_DTM"))

        # HOSPITALIZATION COUNTS and FLAGS
        covid_adt_df = self.get_norm_patient_type(covid_adt_df, "PATIENT_PROCESS_TYPE")
        covid_adt_df = covid_adt_df.withColumn("HOSPITALIZATION_DTM", F.greatest(F.col("ADMIT_DTM"), F.col("NFER_DTM")))

        covid_adt_df = covid_adt_df.withColumn("HOSPITALIZED_DAYS", F.datediff(F.from_unixtime("DISCHARGE_DTM"),
                                               F.from_unixtime("HOSPITALIZATION_DTM")))

        covid_adt_df = covid_adt_df.withColumn("INPATIENT_DAYS", F.when(F.col("PATIENT_TYPE").isin("inpatient"),
                                                F.col("HOSPITALIZED_DAYS")).otherwise(F.lit(0)))

        covid_adt_df = covid_adt_df.withColumn("ICU_DAYS", F.when(F.col("LOCATION_BED_TYPE").isin("ICU"),
                                                F.col("HOSPITALIZED_DAYS")).otherwise(F.lit(0)))

        covid_adt_df = covid_adt_df.withColumn("PCU_DAYS", F.when(F.col("LOCATION_BED_TYPE").isin("PCU"),
                                               F.col("HOSPITALIZED_DAYS")).otherwise(F.lit(0)))

        covid_adt_df = covid_adt_df.groupBy("NFER_PID", "NFER_DTM").agg(
            F.max("INPATIENT_DAYS").alias("HOSPITALIZED_DAYS"),
            F.max("ICU_DAYS").alias("ICU_DAYS"),
            F.max("PCU_DAYS").alias("PCU_DAYS")
        )

        covid_adt_df = covid_adt_df.withColumn("HOSPITALIZED_FLAG",
                                               F.when(F.col("HOSPITALIZED_DAYS") > 0, F.lit("Y")).otherwise(F.lit("N")))
        covid_adt_df = covid_adt_df.withColumn("ICU_FLAG",
                                               F.when(F.col("ICU_DAYS") > 0, F.lit("Y")).otherwise(F.lit("N")))

        # MERGE WITH COVID LAB DATA
        covid_lab_data_df = covid_lab_data_df.join(F.broadcast(covid_adt_df), ["NFER_PID", "NFER_DTM"], "left")
        covid_lab_data_df = covid_lab_data_df.withColumn("HOSPITALIZED_FLAG",
                                                         F.coalesce(F.col("HOSPITALIZED_FLAG"), F.lit("N")))
        covid_lab_data_df = covid_lab_data_df.withColumn("ICU_FLAG", F.coalesce(F.col("ICU_FLAG"), F.lit("N")))
        covid_lab_data_df = covid_lab_data_df.withColumn("HOSPITALIZED_DAYS",
                                                         F.coalesce(F.col("HOSPITALIZED_DAYS"), F.lit(0)))
        covid_lab_data_df = covid_lab_data_df.withColumn("ICU_DAYS", F.coalesce(F.col("ICU_DAYS"), F.lit(0)))
        covid_lab_data_df = covid_lab_data_df.withColumn("PCU_DAYS", F.coalesce(F.col("PCU_DAYS"), F.lit(0)))
        covid_lab_data_df = covid_lab_data_df.distinct()

        # PATIENT DECEASED STATUS
        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT", DIM_HEADERS)
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)
        final_data_df = covid_lab_data_df.join(F.broadcast(dim_patient_df), ["NFER_PID"])
        final_data_df = final_data_df.withColumn("DECEASED_FLAG", F.when(F.col("PATIENT_DEATH_DATE") > F.col("NFER_DTM"),
                                                 F.lit("Y")).otherwise(F.lit("N")))

        # NFER FIELDS
        final_data_df = final_data_df.withColumn("COVID_TEST_TIME", F.col("NFER_DTM"))
        final_data_df = self.calc_nfer_age_for_nfer_dtm(final_data_df)

        col_drop = ["RESULT_TXT", "LOCATION_SITE_NAME", "PATIENT_DEATH_DATE", "BIRTH_DATE", "PATIENT_DECEASED_FLAG"]
        final_data_df = final_data_df.drop(*col_drop).distinct()


        # COUNTERS
        extra_counters = {'covid_positive': F.col("COVID_POSITIVE_FLAG") == F.lit("Y")}
        self.write_versioned_syn_data(syn_config, final_data_df, extra_counters)


if __name__ == '__main__':

    out_table = 'covid_comprehensive_summary'

    start = time()
    obj = CovidFull()
    syn_config = obj.SYSTEM_DICT["syn_tables"][out_table]
    obj.generate_covid_comprehensive_summary(syn_config)
    print("{} compelte! : Total Time = {} Mins".format(out_table, round((time() - start)/60)))
