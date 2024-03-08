import os
import json
from time import time
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, udf, Window
from pyspark.sql.types import ArrayType, FloatType, StringType

from core.config_vars import RunMode
from spark_syn_datagen.synthetic_summaries import SynDataGenJob
from spark_syn_datagen.ecg_meta_maps import ECGMetaMaps


DIM_HEADERS = ["NFER_PID", "BIRTH_DATE", "PATIENT_CLINIC_NBR_ISSUING_SITE", "PATIENT_DEATH_DATE",
               "PATIENT_DECEASED_FLAG", "PATIENT_ETHNICITY_NAME", "PATIENT_GENDER_NAME", "PATIENT_NEONATAL_BIRTHWEIGHT",
               "PATIENT_PRIMARY_ZIPCODE", "PATIENT_RACE_NAME", "AGE_AT_FIRST_ENCOUNTER", "FIRST_ENCOUNTER_DATE"]


def array_to_string(my_list):

    my_list = list(set(my_list))

    if len(my_list) == 1:
        return str(my_list[0])
    elif len(my_list) > 1:
        return '{' + '|'.join([str(elem) for elem in my_list]) + '}'
    else:
        return ''


class ECG(ECGMetaMaps):

    def __init__(self):
        super().__init__()

    def generate_ecg_summary(self):
        self.init_spark_session()
        self.generate_meta_maps()
        wave_df = self.load_latest_meta_maps()
        dim_df = self.read_versioned_datagen_dir("DIM_PATIENT", version=self.RUN_VERSION, columns=DIM_HEADERS)

        dim_df = dim_df.groupby('NFER_PID').agg(F.expr('collect_list(BIRTH_DATE)').alias('BIRTH_DATE'),
                                    F.expr('collect_list(PATIENT_CLINIC_NBR_ISSUING_SITE)').alias('PATIENT_CLINIC_NBR_ISSUING_SITE'),
                                    F.expr('collect_list(PATIENT_DEATH_DATE)').alias('PATIENT_DEATH_DATE'),
                                    F.expr('collect_list(PATIENT_DECEASED_FLAG)').alias('PATIENT_DECEASED_FLAG'),
                                    F.expr('collect_list(PATIENT_NEONATAL_BIRTHWEIGHT)').alias('PATIENT_NEONATAL_BIRTHWEIGHT'),
                                    F.expr('collect_list(PATIENT_PRIMARY_ZIPCODE)').alias('PATIENT_PRIMARY_ZIPCODE'),
                                    F.expr('collect_list(AGE_AT_FIRST_ENCOUNTER)').alias('AGE_AT_FIRST_ENCOUNTER'),
                                    F.expr('collect_list(FIRST_ENCOUNTER_DATE)').alias('FIRST_ENCOUNTER_DATE'),
                                    F.expr('collect_list(PATIENT_ETHNICITY_NAME)').alias('PATIENT_ETHNICITY_NAME'),
                                    F.expr('collect_list(PATIENT_GENDER_NAME)').alias('PATIENT_GENDER_NAME'),
                                    F.expr('collect_list(PATIENT_RACE_NAME)').alias('PATIENT_RACE_NAME'))

        array_to_string_udf = F.udf(array_to_string, StringType())
        for col in DIM_HEADERS:
            if col != 'NFER_PID':
                dim_df = dim_df.withColumn(col, array_to_string_udf(dim_df[col]))

        df = wave_df.join(dim_df, on=['NFER_PID'], how='left')
        df = self.calc_nfer_age_for_nfer_dtm(df)

        out_dir = os.path.join(self.LOCAL_ROOT_DIR, self.RUN_VERSION, "WAVEFORM", "FACT_ECG_WAVEFORM", "CSV_2")
        os.system("rm -rf {}".format(out_dir))
        df.write.option("delimiter", ",").csv(out_dir)
        out_info_file = "{}/ecg_waveform.csv".format('/'.join(out_dir.split('/')[:-1]))
        os.system("echo -e {} | cat - {}/*.csv > {}".format((',').join(df.columns), out_dir, out_info_file))
        gs_out_info_file = f'{self.DATA_DIR}/WAVEFORM/FACT_ECG_WAVEFORM/ecg_waveform.csv'
        self.copy_file(out_info_file, gs_out_info_file)
        print("ECG summary File: {}".format(gs_out_info_file))


if __name__ == '__main__':
    start = time()
    obj = ECG()
    obj.generate_ecg_summary()
    print("complete! : Total Time = {} Mins".format(round((time() - start)/60)))
