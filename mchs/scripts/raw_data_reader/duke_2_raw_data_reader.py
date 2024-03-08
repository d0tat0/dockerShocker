import os
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from raw_data_reader.base_data_reader import BaseRawDataReader
from core.config_vars import INCR_COUNTER


class Duke2RawDataReader(BaseRawDataReader):

    def __init__(self, data_reader, config_file=None):
        super().__init__(data_reader, config_file)

    def read_raw_data(self, source, source_path=None, version=None):
        source = source.lower()
        version = self.data_reader_obj.RUN_VERSION
        if not source_path:
            source_path = self.data_reader_obj.get_raw_source_file_list(source, version, "data")
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))

        source_schema = self.build_duke_data_schema(source)
        spark = self.data_reader_obj.init_spark_session()

        raw_source_df = None
        for idx, _source_list in source_path:
            _source_df = spark.read.json(_source_list, source_schema, mode='FAILFAST')
            _source_df = _source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            _source_df = _source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not raw_source_df:
                raw_source_df = _source_df
            else:
                raw_source_df = raw_source_df.unionByName(_source_df)

        # Convert column names to upper
        # We want all column names to be in upper case,
        # For Duke, json has lowercase/camelcase  keys and hence amc_schema has to be in the same case
        raw_source_df = raw_source_df.select([F.col(col).alias(col.upper()) for col in raw_source_df.columns])
        for field in raw_source_df.schema.fields:
            if isinstance(field.dataType, StructType):
                new_schema = self.get_upper_case_schema(field.dataType)
                raw_source_df = raw_source_df.withColumn(field.name, F.col(field.name).cast(new_schema))
            if isinstance(field.dataType, ArrayType):
                new_schema = self.get_upper_case_schema(field.dataType.elementType)
                new_schema = ArrayType(new_schema)
                raw_source_df = raw_source_df.withColumn(field.name, F.col(field.name).cast(new_schema))

        return raw_source_df

    def get_upper_case_schema(self, field):
        new_schema = StructType()
        if isinstance(field, StructType):
            for f in field:
                new_schema.add(f.name.upper(), self.get_upper_case_schema(f.dataType))
        else:
            return field
        return new_schema

    def build_duke_data_schema(self, source):
        if source in self.data_reader_obj.SYSTEM_DICT["data_tables"]:
            schema_config = self.data_reader_obj.SYSTEM_DICT["data_tables"][source]
        else:
            schema_config = self.data_reader_obj.SYSTEM_DICT["info_tables"][source]
        schema = self.parse_type_from_config(schema_config["amc_schema"], None)
        return schema

    def parse_type_from_config(self, type_config, field):
        if isinstance(type_config, str):
            spark_field_type = self.data_reader_obj.to_spark_types(type_config)
            return StructField(field, spark_field_type, True)
        elif isinstance(type_config, list):
            if isinstance(type_config[0], dict):
                schema = ArrayType(self.parse_type_from_config(type_config[0], None))
            else:
                spark_field_type = self.data_reader_obj.to_spark_types(type_config[0])
                schema = ArrayType(spark_field_type)
        elif isinstance(type_config, dict):
            schema = StructType()
            for key_field in type_config:
                schema.add(self.parse_type_from_config(type_config[key_field], key_field))
        else:
            raise ValueError("Schema Error")
        if field is None:
            return schema
        return StructField(field, schema, True)

    def read_raw_dim_source(self, source):
        source = source.lower()
        version = self.data_reader_obj.RUN_VERSION
        source_path = self.data_reader_obj.get_raw_source_file_list(source, version, "info")
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))

        source_schema = self.build_duke_data_schema(source)
        spark = self.data_reader_obj.init_spark_session()

        raw_source_df = None
        for idx, _source_list in source_path:
            _source_df = spark.read.json(_source_list, source_schema)
            _source_df = _source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            _source_df = _source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not raw_source_df:
                raw_source_df = _source_df
            else:
                raw_source_df = raw_source_df.unionByName(_source_df)

        raw_source_df = raw_source_df.select([F.col(col).alias(col.upper()) for col in raw_source_df.columns])
        return raw_source_df
