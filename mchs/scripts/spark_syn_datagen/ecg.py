import os
import json
from time import time
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.window import Window
from core.config_vars import RunMode
from spark_syn_datagen.synthetic_summaries import SynDataGenJob
from spark_jobs.orchestrator import Orchestrator
from spark_syn_datagen.ecg_meta_maps import ECGMetaMaps


ECG_HEADERS = ["NFER_PID", "NFER_DTM", "NFER_AGE", "HEART_RATE", "P_WAVE_NBR", "PR_WAVE_NBR",
               "QT_INTERVAL", "QTC_CALCULATION", "QTF_CALCULATION", "R_WAVE_NBR", "FILE_ID",
               "ECG_INTERPRET_SEQ_NBR","ECG_INTERPRET_TEXT"]

OUT_TABLE = 'ecg_summary'


class ECG(ECGMetaMaps):

    def __init__(self):
        super().__init__()
        # self.skip_if_exists = False

    def append_labelled_data(self, df):
        file_path = os.path.join(self.SYSTEM_DICT["ees_resources_dir"], "ecg-data-labelled-75-dis-names.csv")
        labelled_df = self.read_csv_to_df(file_path)
        print(f"LDF COUNT: {labelled_df.count()}")
        print(f"ECG DF COUNT: {df.count()}")
        common_df = df.join(labelled_df, ['NFER_PID', 'NFER_DTM'])
        print(f"Common COUNT: {common_df.count()}")
        df = df.join(labelled_df, ['NFER_PID', 'NFER_DTM'], 'left')
        return df

    def generate_ecg_summary(self):
        self.init_spark_session()
        syn_config = self.SYSTEM_DICT["syn_tables"][OUT_TABLE]
        wave_df = self.load_latest_meta_maps()

        # read fact_ecg table
        ecg_df = self.read_versioned_datagen_dir(source="FACT_ECG", columns=ECG_HEADERS)
        ecg_df = ecg_df.withColumnRenamed('NFER_DTM', 'NFER_DTM_ECG')

        # take only vaild non negative seq nos
        ecg_df = ecg_df.withColumn("ECG_INTERPRET_SEQ_NBR",F.col("ECG_INTERPRET_SEQ_NBR").cast("int"))

        # rename null or negative seq numbers to very big values like 9999999 so that they end up to the very end in sorting order
        ecg_df = ecg_df.withColumn("my_seq_num",F.when(ecg_df.ECG_INTERPRET_SEQ_NBR.isNull(),F.lit(999999))
                                                            .when(ecg_df.ECG_INTERPRET_SEQ_NBR<0,F.lit(9999999))
                                                            .otherwise(ecg_df.ECG_INTERPRET_SEQ_NBR))

        """
        # keep track of the probable columns as it needs to be prepended
        newdf = ecg_df.filter(F.col("ECG_INTERPRET_TEXT").isin(["Possible %0A","Probable %0A"])).select(["NFER_PID","NFER_DTM_ECG","ECG_INTERPRET_TEXT"])
        newdf = newdf.withColumn('ECG_INTERPRET_TEXT', F.expr("substring(ECG_INTERPRET_TEXT,0,length(ECG_INTERPRET_TEXT)-3)"))
        newdf = newdf.withColumnRenamed("ECG_INTERPRET_TEXT","PREFIX")
        newdf.show()
        """
        # remove rows where 1st interpret is Probable and Possible and take the next comment
        #ecg_df = ecg_df.filter(F.col("ECG_INTERPRET_TEXT") != "Possible %0A")
        #ecg_df = ecg_df.filter(F.col("ECG_INTERPRET_TEXT") != "Probable %0A")

        # remove numeric ECG_INTERPRET_TEXT
        ecg_df = ecg_df.withColumn("temp",F.col("ECG_INTERPRET_TEXT").cast("int").isNull())
        ecg_df = ecg_df.withColumn("ECG_INTERPRET_TEXT",F.when(ecg_df.temp == "false",F.lit(None)).otherwise(ecg_df.ECG_INTERPRET_TEXT))
        ecg_df = ecg_df.drop("temp")

        window = Window.partitionBy("NFER_PID", "NFER_DTM_ECG").orderBy("my_seq_num").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        ecg_df = ecg_df.withColumn("NFER_AGE", F.first(ecg_df['NFER_AGE'],ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("HEART_RATE", F.first(ecg_df['HEART_RATE'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("P_WAVE_NBR", F.first(ecg_df['P_WAVE_NBR'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("PR_WAVE_NBR", F.first(ecg_df['PR_WAVE_NBR'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("QT_INTERVAL", F.first(ecg_df['QT_INTERVAL'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("QTC_CALCULATION", F.first(ecg_df['QTC_CALCULATION'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("QTF_CALCULATION", F.first(ecg_df['QTF_CALCULATION'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("R_WAVE_NBR", F.first(ecg_df['R_WAVE_NBR'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("FILE_ID", F.first(ecg_df['FILE_ID'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("ECG_MAIN_RHYTHM", F.first(ecg_df['ECG_INTERPRET_TEXT'], ignorenulls=True).over(window))
        ecg_df = ecg_df.withColumn("ECG_INTERPRET_SEQ_NBR",
                                   F.first(ecg_df['ECG_INTERPRET_SEQ_NBR'], ignorenulls=True).over(window))

        ecg_df = ecg_df.dropDuplicates(["NFER_PID","NFER_DTM_ECG"])

        # remove %0A trailing, remove *** from beginning and end and trim the string
        ecg_df = ecg_df.withColumn('ECG_MAIN_RHYTHM', F.expr("substring(ECG_MAIN_RHYTHM,0,length(ECG_MAIN_RHYTHM)-4)"))
        ecg_df = ecg_df.withColumn('ECG_MAIN_RHYTHM', F.regexp_replace(F.col('ECG_MAIN_RHYTHM'), '\*', ''))
        ecg_df = ecg_df.withColumn('ECG_MAIN_RHYTHM', F.trim(F.col('ECG_MAIN_RHYTHM')))

        """
        if we try to map waveform at time t to nearest fact_ecg record, there might be records
        of fact_ecg at timestamp t0 and t2  where
            t0<t<t2 and t- t0 = t2-t .. but there are only 8 such cases so the impact is very little
        """
        print(f'waveform df distinct nfer_pid count {wave_df.select("NFER_PID").distinct().count()}')  # 2443275
        dist_df = wave_df.join(ecg_df, ['NFER_PID'],how='left')
        dist_df = dist_df.withColumn("distance", F.abs(wave_df['NFER_DTM'] - ecg_df['NFER_DTM_ECG']))
        min_df = dist_df.groupBy(dist_df['NFER_PID'], dist_df['NFER_DTM']).agg(F.min(dist_df.distance).alias('distance'))
        df = Orchestrator.join_with_null(dist_df,min_df,['NFER_PID', 'NFER_DTM', 'distance'],how='right').withColumn('WAVEFORM_FLAG', F.lit('y'))
        agg_exprs = [F.first(col_name).alias(col_name) for col_name in df.columns if col_name != "EXT_ECG_ID"]
        df = df.orderBy("EXT_ECG_ID", "NFER_DTM_ECG").groupBy("EXT_ECG_ID").agg(*agg_exprs)
        print(f'Final df distinct nfer_pid count {wave_df.select("NFER_PID").distinct().count()}')
        # self.write_versioned_syn_data(syn_config, df)
        # df = self.read_versioned_syn_data(OUT_TABLE)
        df = self.append_labelled_data(df)
        self.write_versioned_syn_data(syn_config, df)


if __name__ == '__main__':
    start = time()
    obj = ECG()
    obj.generate_ecg_summary()
    print("{} compelte! : Total Time = {} Mins".format(OUT_TABLE, round((time() - start)/60)))