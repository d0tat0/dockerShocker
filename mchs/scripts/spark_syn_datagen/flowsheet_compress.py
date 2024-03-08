import os
from time import time
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


"""
|NFER_VARIABLE_NAME|NFER_NORMALISED_VALUE|
"""


class FlowsheetsCompression(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def compress(self, variable_name_col="NFER_VARIABLE_NAME", result_col="NFER_NORMALISED_VALUE"):

        self.init_spark_session()

        # source_df = self.read_final_dir('/data3/ORCHESTRATOR/4.005/DATAGEN/flo/')
        syn_config = self.SYSTEM_DICT["harmonized_tables"]["harmonized_flowsheets"]
        nfer_schema = syn_config['nfer_schema']

        source_df = self.read_versioned_datagen_dir('FACT_FLOWSHEETS', version="4.005", columns=nfer_schema)

        patient_window = Window.partitionBy("NFER_PID", variable_name_col).orderBy("NFER_DTM")
        source_df = source_df.withColumn("DATE", F.from_unixtime(F.col("NFER_DTM")))

        # Repeated col will be TRUE if previous result was same
        source_df = source_df.withColumn("REPEATED",
                                           F.col(result_col) == F.lag(result_col).over(patient_window))
        # gate the datediff between two cols
        source_df = source_df.withColumn("DATEDIFF_FROM_PREV",
                                           F.datediff(F.col("DATE"), F.lag("DATE", 1).over(patient_window)))

        # the first record in the datediff_from_prev will have null as value as there was no previous value
        source_df = source_df.na.fill(value=-1, subset=["DATEDIFF_FROM_PREV"])

        # the first record in the REPEATED will have null as value as there was no previous value
        source_df = source_df.na.fill(value=False, subset=["REPEATED"])

        source_df = source_df.withColumn("DATEDIFF_FROM_PREV",
                                           F.when(F.col("DATEDIFF_FROM_PREV") == F.lit(""), -1)
                                           .otherwise(F.col("DATEDIFF_FROM_PREV"))
                                           )
        source_df = source_df.withColumn("REPEATED",
                                           F.when(F.col("REPEATED") == F.lit(""), False)
                                           .otherwise(F.col("REPEATED"))
                                           )

        source_df = source_df.filter(
            ~((F.col('REPEATED') == True) & (F.col("DATEDIFF_FROM_PREV") == 0))
        )

        source_df = self.assign_file_id_for_nfer_pid(source_df, "DIM_PATIENT")

        # WRITE DATA
        source_dir_name = syn_config["dir_name"].split("/")[-1].replace('HARMONIZED', 'COMPRESSED')
        self.write_final_dir("SYN_DATA", source_df, nfer_schema, source_dir_name)
        print("HARMONIZED_FLOWSHEET GENERATED")


if __name__ == '__main__':

    start = time()
    obj = FlowsheetsCompression()
    obj.compress()
    print("compelte! : Total Time = {} Mins".format(round((time() - start) / 60)))