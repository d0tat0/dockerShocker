import os
from time import time
from pyspark.sql import *
from pyspark.sql.functions import *
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


class ProcedureNormalizer(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def run(self):

        self.init_spark_session()
        dim_file_name = self.SYSTEM_DICT["info_tables"]["dim_procedure_code"]["files"][0]
        dim_file_path = os.path.join(self.DATA_DIR, "DATAGEN/DIM_INFO", dim_file_name)
        if not os.path.exists(dim_file_path):
            print("DIM_PROCEDURE_CODE not available, hence skipping.")
            return

        source_df = self.read_versioned_datagen_dir("FACT_PROCEDURES")
        if source_df is None:
            print("FACT_PROCEDURES not available, hence skipping.")
            return

        source_df = self.data_frame_proc(source_df, "PROCEDURE_CODE", "PROCEDURE_CODE_DK", "NFER_PID")
        dim_df = self.read_versioned_dim_table("DIM_PROCEDURE_CODE", latest_only=False)
        if "NORMALIZED_PROCEDURE_DESCRIPTION" in dim_df.columns:
            print("DIM_PROCEDURE_CODE already normalized, hence skipping.")
            return

        normalized_df = source_df.join(dim_df, ["PROCEDURE_CODE_DK", "PROCEDURE_CODE"])
        normalized_df = normalized_df.select("PROCEDURE_CODE", "PROCEDURE_NAME","PROCEDURE_DESCRIPTION")
        normalized_df = normalized_df.withColumnRenamed("PROCEDURE_DESCRIPTION", "NORMALIZED_PROCEDURE_DESCRIPTION")
        normalized_df = normalized_df.withColumnRenamed("PROCEDURE_NAME", "NORMALIZED_PROCEDURE_NAME")

        final_dim_df = dim_df.join(normalized_df, ["PROCEDURE_CODE"])
        backup_file_name = dim_file_name.replace(".txt", ".bkp")
        self.rename_file(dim_file_path, backup_file_name)
        self.write_df_to_csv(final_dim_df, dim_file_name)


    def data_frame_proc(self, data_frame:DataFrame, PROCEDURE_CODE, PROCEDURE_CODE_DK, NFER_PID):
        data_frame = data_frame.groupBy(PROCEDURE_CODE, PROCEDURE_CODE_DK).agg(count_distinct(NFER_PID).alias("count"))
        w2 = Window.partitionBy(PROCEDURE_CODE).orderBy(col("count").desc())
        code_dk_df = data_frame.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row")
        return code_dk_df


if __name__ == '__main__':

    start = time()
    obj = ProcedureNormalizer()
    obj.run()