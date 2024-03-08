import os
import random
from pyspark.sql import functions as F
from spark_jobs.orchestrator import Orchestrator


class DeltaVersion(Orchestrator):

    def __init__(self):
        super().__init__()
        self.init_spark_session()

    def fix_delta(self):
        spark = self.SPARK_SESSION
        delta_path = os.path.join(self.WRITE_ROOT_DIR, str(self.RUN_VERSION), "DELTA_TABLES", "FACT_TABLES")
        fmv_path = os.path.join(self.WRITE_ROOT_DIR, str(self.RUN_VERSION), "FACT_MAPS_VERSIONED.parquet")
        table_list = ["DIM_APPOINTMENT_INDICATION_BRIDGE", "DIM_OB_DELIVERY_DISCHARGE_MED_BRIDGE", "DIM_OB_DELIVERY_TEAM_HEALTHCARE_PROVIDER_ROLE_BRIDGE", "DIM_PATHOLOGY_DIAGNOSIS_CODE_BRIDGE", "DIM_SURGICAL_CASE_NOTE_BRIDGE", "FACT_ADMIT_DISCHARGE_TRANSFER_LOCATION", "FACT_ALLERGIES", "FACT_APPOINTMENT", "FACT_CARE_PLAN", "FACT_CARE_PLAN_CURRENT_DIAGNOSIS", "FACT_CARE_PLAN_CURRENT_MEDICATION", "FACT_CLAIMS", "FACT_CLAIMS_ITEM_DIAGNOSIS", "FACT_CLAIMS_ITEM_PROCEDURE", "FACT_CLINICAL_DOCUMENTS", "FACT_DEVICEMANUFACTURER", "FACT_DIAGNOSIS", "FACT_ECG", "FACT_ECG_WAVEFORM", "FACT_ECG_WAVEFORM_MATRIX", "FACT_ECG_WAVEFORM_MATRIX_DETAIL", "FACT_ECG_WAVEFORM_RESTING_MEASUREMENT", "FACT_ECG_WAVEFORM_TSTDIAGNOSISDETAIL", "FACT_ECHO_RESULTS", "FACT_ECHO_TEST", "FACT_ENCOUNTERS", "FACT_FAMILY_MEDICAL_HISTORY", "FACT_FLOWSHEETS"]
        for _path in self.glob(delta_path):
            table_name = _path.strip("/").split("/")[-1].split(".")[0]
            _path = _path.strip("/")
            if table_name == "FACT_FLOWSHEETS":
                print(f'Reading -> {_path}')
                df = spark.read.format("delta").load(_path)
                print(f'Reading -> {os.path.join(fmv_path, table_name)}')
                fmv = spark.read.parquet(os.path.join(fmv_path, table_name))
                df = df.drop("VERSION").drop("UPDATED_BY")
                df = df.join(fmv.select(["NFER_PID", "ROW_ID", "VERSION", "UPDATED_BY"]), ["NFER_PID", "ROW_ID"], "LEFT")
                write_path = os.path.join(self.WRITE_ROOT_DIR, "LATEST_DELTA_TABLES", "DELTA_TABLES", "FACT_TABLES",
                                          f"{table_name}.parquet")
                self.write_df_to_delta_lake(df, write_path)
                print('Overwriting with latest delta')
                self.overwrite_latest_delta(write_path, _path)

    def fix_dim_pat(self):
        spark = self.SPARK_SESSION
        delta_path = os.path.join(self.WRITE_ROOT_DIR, str(self.RUN_VERSION), "DELTA_TABLES", "FACT_TABLES",
                                  "DIM_PATIENT.parquet")
        dmv_path = os.path.join(self.WRITE_ROOT_DIR, str(self.RUN_VERSION), "DATAGEN.parquet", "DIM_MAPS_VERSIONED")
        print(f'Reading -> {delta_path}')
        df = spark.read.format("delta").load(delta_path)
        print(f'Reading -> {dmv_path}')
        fmv = spark.read.parquet(dmv_path)
        df = df.drop("VERSION").drop("UPDATED_BY")
        df = df.join(fmv.select(["NFER_PID", "ROW_ID", "VERSION", "UPDATED_BY"]), ["NFER_PID", "ROW_ID"], "LEFT")
        write_path = os.path.join(self.WRITE_ROOT_DIR, "LATEST_DELTA_TABLES", "DELTA_TABLES", "FACT_TABLES",
                                  "DIM_PATIENT.parquet")
        print(f'Writing -> {write_path}')
        self.write_df_to_delta_lake(df, write_path)
        self.overwrite_latest_delta(write_path, delta_path)

    def run(self):
        if self.options.run == "fix_delta":
            self.fix_delta()
        if self.options.run == "fix_dim_pat":
            self.fix_dim_pat()


if __name__ == '__main__':
    DeltaVersion().run()

