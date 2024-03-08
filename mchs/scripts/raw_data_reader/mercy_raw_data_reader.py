import fnmatch
import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType, StructField, StructType, IntegerType, StringType, DoubleType, BooleanType, DateType
from raw_data_reader.base_data_reader import BaseRawDataReader
from string import Template
from core.config_vars import DEFAULT_COUNTER, PIPE_SEP, INCR_COUNTER

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructField, StructType

from raw_data_reader.base_data_reader import BaseRawDataReader


type_dict = {
    "IntegerType": IntegerType(),
    "StringType": StringType(),
    "LongType": LongType(),
    "DoubleType": DoubleType(),
    "DATE": DateType(),
    "BOOLEAN": BooleanType()
}

class MercyRawDataReader(BaseRawDataReader):

    def __init__(self, data_reader, config_file=None):
        super().__init__(data_reader, config_file)

    def read_raw_data(self, source, source_path=None, version=None):
        if not source_path:
            source_path = self.data_reader_obj.get_raw_source_file_list(source, version, "data")
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))
        source_schema = self.build_data_schema(source, source_path[0][1][0], "data_tables")
        parse_mode = "FAILFAST"
        raw_source_df = None
        for idx, _source_list in source_path:
            _source_df = self.data_reader_obj.read_csv_to_df(_source_list, source_schema, parse_mode=parse_mode)
            _source_df = _source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            _source_df = _source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not raw_source_df:
                raw_source_df = _source_df
            else:
                raw_source_df = raw_source_df.unionByName(_source_df)
        return raw_source_df

    def build_data_schema(self, source, sample_file, type):
        header_list, _ = self.data_reader_obj.read_headers(sample_file)
        schema = StructType()
        for field in header_list:
            datatype = self.data_reader_obj.SYSTEM_DICT[type][source.lower()]['amc_schema'].get(field, None)
            spark_field_type = type_dict[datatype] if datatype else type_dict["StringType"]
            schema.add(StructField(field, spark_field_type, True))
        return schema

    def read_raw_dim_source(self, source):
        version = self.data_reader_obj.RUN_VERSION
        source_path = self.data_reader_obj.get_raw_source_file_list(source, version, "info")
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))

        source_schema = self.build_data_schema(source, source_path[0][1][0], "info_tables")

        raw_source_df = None
        for idx, _source_list in source_path:
            _source_df = self.data_reader_obj.read_csv_to_df(_source_list, source_schema)
            _source_df = _source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            _source_df = _source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not raw_source_df:
                raw_source_df = _source_df
            else:
                raw_source_df = raw_source_df.unionByName(_source_df)

        raw_source_df = raw_source_df.select([F.col(col).alias(col.upper()) for col in raw_source_df.columns])
        return raw_source_df
