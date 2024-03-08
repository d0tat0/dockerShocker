from time import time
from pyspark.sql import functions as F, SparkSession, DataFrame
from spark_syn_datagen.synthetic_summaries import SynDataGenJob

from core.config_vars import RunMode

CONFLICT_COLNAME = "CONFLICT_FOUND"
CONFLICT_TRUE_VALUE = "Y"
CONFLICT_FALSE_VALUE = "N"
CONFLICT_ACROSS_VERSION_VALUE = "C"

CONFLICT_COLS = [
    "PATIENT_DEATH_DATE",
    "PATIENT_DECEASED_FLAG",
    "PATIENT_ETHNICITY_NAME",
    "PATIENT_GENDER_NAME",
    "PATIENT_NEONATAL_BIRTHWEIGHT",
    "PATIENT_RACE_NAME",
    "AGE_AT_FIRST_ENCOUNTER",
    "FIRST_ENCOUNTER_DATE",
    "PATIENT_PRIMARY_ZIPCODE"
]


class SynDimPat(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def get_across_version_conflict(self, bigdf:DataFrame, conflict_cols):
        """
        Given a dataframe find conflict for the given conflict_cols,where
        windows are calculated based on partition by cols
        calculate if any row changed across version

        returns a dataframe of nfer_pid to be
        """
        df = bigdf.filter(F.col("PATIENT_MERGED_FLAG") == "N")
        df = df.withColumn("HASH",
                            F.sha2(F.concat_ws("|", F.array(conflict_cols + ["NFER_PID"])), 256))
        df = df.groupBy(["NFER_PID"]).agg(F.countDistinct("HASH").alias("uniq_cnt"))
        df = df.filter(F.col("uniq_cnt") > 1).drop("uniq_cnt")

        return df

    def run(self, syn_config):
        self.init_spark_session()
        df = self.read_versioned_datagen_dir(source="DIM_PATIENT", latest_only=False)

        # get the df of patients where there is a conflict in column values across version
        across_version_conflict_df = self.get_across_version_conflict(df, CONFLICT_COLS)

        print(f"Across version conflicts {across_version_conflict_df.count()}")

        df = df.filter(F.col("UPDATED_BY") == F.lit(0.0))

        # get patients with same version conflict
        conflict_df = df.groupBy("NFER_PID").agg(F.count("NFER_PID").alias("count"))
        conflict_df = conflict_df.filter(F.col("count") > 1).drop("count")
        conflict_df = conflict_df.join(df, on="NFER_PID", how="inner")
        conflict_df = conflict_df.withColumn("HASH",
                                             F.sha2(F.concat_ws("|", F.array(CONFLICT_COLS + ["NFER_PID"])), 256))
        conflict_df = conflict_df.groupBy(["NFER_PID"]).agg(F.countDistinct("HASH").alias("uniq_cnt"))
        conflict_df = conflict_df.filter(F.col("uniq_cnt") > 1).drop("uniq_cnt")
        conflict_cnt = conflict_df.count()
        print(f"conflict count {conflict_cnt}")

        conflict_df = conflict_df.withColumn(CONFLICT_COLNAME, F.lit(CONFLICT_TRUE_VALUE))

        df1 = df.filter(F.col("PATIENT_MERGED_FLAG") == "N")
        df2 = df.filter(F.col("PATIENT_MERGED_FLAG") != "N")

        only_y_nfer_pid_df = df2.select("NFER_PID").subtract(df1.select("NFER_PID"))

        df2 = df2.join(F.broadcast(only_y_nfer_pid_df), on="NFER_PID", how="inner")
        all_y = df2.count()
        print(f"patients where patient_merged_flag=y  {all_y}")

        out_df = df2.unionByName(df1)
        out_df = out_df.sort(["NFER_PID"] + CONFLICT_COLS)
        out_df = out_df.dropDuplicates(subset=["NFER_PID"])

        out_df = out_df.join(conflict_df, on="NFER_PID", how="left")
        out_df = out_df.fillna(CONFLICT_FALSE_VALUE, CONFLICT_COLNAME)

        cross_conflict_col = "cross_version_conflict"
        across_version_conflict_df = across_version_conflict_df.withColumn(cross_conflict_col,
                                                                           F.lit(CONFLICT_ACROSS_VERSION_VALUE)
                                                                           )
        out_df = out_df.join(across_version_conflict_df, on="NFER_PID", how="left")
        out_df = out_df.withColumn(CONFLICT_COLNAME,
                                   F.when(F.col(cross_conflict_col).isNotNull(),F.col(cross_conflict_col))
                                   .otherwise(F.col(CONFLICT_COLNAME))
                                   )
        out_df = out_df.drop(cross_conflict_col)

        print(f"Count of final df {out_df.count()}")

        self.RUN_MODE = RunMode.Full.value
        self.write_versioned_syn_data(syn_config, out_df)


if __name__ == '__main__':
    obj = SynDimPat()
    out_table = 'dim_patient_summary'
    start = time()
    syn_config = obj.SYSTEM_DICT["syn_tables"][out_table]
    obj.run(syn_config)
    print("{} compelte! : Total Time = {} Mins".format(out_table, round((time() - start) / 60)))
