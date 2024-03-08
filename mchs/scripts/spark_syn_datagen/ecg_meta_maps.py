import os
import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window

from core.config_vars import RunMode
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


class ECGMetaMaps(SynDataGenJob):

    def __init__(self):
        super().__init__()
        self._waves_meta_schema = self.SYSTEM_DICT["version_tables"]["meta_maps_versioned"]["nfer_schema"]
        self._waves_meta_type_schema = self.SYSTEM_DICT["version_tables"]["meta_maps_versioned"]["type_schema"]

    def load_last_meta_maps(self,):
        meta_map_dir = self.last_data_dir("WAVEFORM", "META_MAPS_VERSIONED")
        if meta_map_dir and self.glob(meta_map_dir):
            waves_map_df = self.read_final_dir(meta_map_dir)
        else:
            waves_map_df = self.create_empty_dataframe(self._waves_meta_schema, self._waves_meta_type_schema)

        return waves_map_df

    def validate_ecg_id(self, wave_df):
        duplicates = wave_df.groupBy(F.col('EXT_ECG_ID')).count().filter(F.col('count') > 1)

        if duplicates.count() > 0:
            print(duplicates.show())
            raise ValueError("Duplicate ECG IDs")

    def generate_ecg_id(self, wave_df):
        waves_map_df = self.load_last_meta_maps()
        wave_df = wave_df.join(F.broadcast(waves_map_df), ['NFER_PID', 'NFER_DTM'], "left")
        wave_df = wave_df.na.fill(0, subset=["EXT_ECG_ID"])
        max_ecg_id = wave_df.agg(F.max("EXT_ECG_ID")).collect()[0][0]
        if not max_ecg_id:
            max_ecg_id = 0
        order_window = Window.partitionBy().orderBy("EXT_ECG_ID", "NFER_PID", "NFER_DTM")
        wave_df = wave_df.withColumn("EXT_ECG_ID", F.when(F.col("EXT_ECG_ID").isin(0),
                                                      F.lit(max_ecg_id) + F.dense_rank().over(order_window))
                                     .otherwise(F.col("EXT_ECG_ID")))
        self.validate_ecg_id(wave_df)
        self.write_final_dir("WAVEFORM", wave_df, self._waves_meta_schema, "META_MAPS_VERSIONED")
        return wave_df

    def generate_meta_maps(self):
        if self.RUN_MODE == RunMode.Delta.value:
            filename = "combined_meta.json"
        else:
            filename = "meta.json"

        waveform_dir = self.data_dir("DATAGEN", "FACT_ECG_WAVEFORM")
        if self.glob(waveform_dir):
            wave_meta_path = "{}/WAVEFORM/FACT_ECG_WAVEFORM/RHYTHM/{}".format(self.DATA_DIR, filename)
            wave_meta = self.get_json_data(wave_meta_path)
            pid_ts_list = [i.split('_') for i in wave_meta.keys() if wave_meta[i]['is_valid']]
            wave_df = self.SPARK_SESSION.createDataFrame(pid_ts_list, ['NFER_PID', 'NFER_DTM'])
            self.generate_ecg_id(wave_df)
        else:
            print("No ECG data found")

    def load_latest_meta_maps(self):
        meta_map_dir = self.latest_data_dir("WAVEFORM", "META_MAPS_VERSIONED")
        if not meta_map_dir or not self.glob(meta_map_dir):
            self.generate_meta_maps()
        waves_map_df = self.read_final_dir(meta_map_dir)
        waves_map_df = waves_map_df.drop("FILE_ID")
        return waves_map_df
