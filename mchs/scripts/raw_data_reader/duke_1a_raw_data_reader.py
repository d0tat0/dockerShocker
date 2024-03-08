import os
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from raw_data_reader.base_data_reader import BaseRawDataReader
from core.config_vars import INCR_COUNTER


class Duke1ARawDataReader(BaseRawDataReader):

    def __init__(self, data_reader, config_file=None):
        super().__init__(data_reader, config_file)

    def read_raw_dim_source(self, source):
        return self.read_data(source, data_type="info")

    def read_raw_data(self, source, source_path=None, version=None):
        return self.read_data(source, data_type="data", source_path=source_path, version=version)

    def read_data(self, source, data_type, source_path=None, version=None):
        source = source.lower()
        version = self.RUN_VERSION
        source_path = self.get_raw_source_file_list(source, version, data_type)
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))

        # # To be used in ENV 2
        # source_schema = self.build_duke_data_schema(source)
        # spark = self.init_spark_session()
        # raw_source_df = spark.read.json(source_path, source_schema)

        # For ENV 1A
        spark = self.init_spark_session()
        # Read the data as string without, This is done because of the invalid data format
        final_raw_source_df = None
        for idx, _source_list in source_path:
            raw_source_df = spark.read.text(source_path)

            # Replace all '//' with '/' to make the data a valid json
            raw_source_df = raw_source_df.withColumn('data', F.regexp_replace("value", r'(?<!(\\))\\\\\"', '\\\\\"'))
            schema_config = self.SYSTEM_DICT[f"{data_type}_tables"][source]["amc_schema"]
            # Create flat schema where nested jsons and arrays are treated as strings,
            # this is done because of the raw data format in Duke 1A
            raw_data_initial_schema = self.build_flat_data_schema(schema_config)
            # Convert the JSON string column to a DataFrame using the defined schema
            # Data will be a single column with a complex structure based on the schema
            raw_source_df = raw_source_df.withColumn("data", F.from_json("data", raw_data_initial_schema))
            # Now the single column from json is exploded to multiple columns
            cols = []
            for col in schema_config:
                cols.append(f"data.{col}")
            raw_source_df = raw_source_df.select(cols)

            # Convert type of nested jsons and arrays from String to corresponding schema
            for column in schema_config:
                if not isinstance(schema_config[column], str):
                    mapped_codes_schema = self.parse_type_from_config(schema_config[column], None)
                    raw_source_df = raw_source_df.withColumn(column, F.from_json(column, mapped_codes_schema))
            raw_source_df = raw_source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            raw_source_df = raw_source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not final_raw_source_df:
                final_raw_source_df = raw_source_df
            else:
                final_raw_source_df = final_raw_source_df.unionByName(raw_source_df)
        return final_raw_source_df

    def build_duke_data_schema(self, source):
        schema_config = self.SYSTEM_DICT["data_tables"][source]
        schema = self.parse_type_from_config(schema_config, None)
        return schema

    def parse_type_from_config(self, type_config, field):
        if isinstance(type_config, str):
            spark_field_type = self.to_spark_types(type_config)
            return StructField(field, spark_field_type, True)
        elif isinstance(type_config, list):
            if isinstance(type_config[0], dict):
                schema = ArrayType(self.parse_type_from_config(type_config[0], None))
            else:
                spark_field_type = self.to_spark_types(type_config[0])
                schema = ArrayType(spark_field_type)
        elif isinstance(type_config, dict):
            schema = StructType()
            for key_field in type_config:
                schema.add(self.parse_type_from_config(type_config[key_field], key_field))
        if field is None:
            return schema
        return StructField(field, schema, True)

    def build_flat_data_schema(self, schema_config):
        # This function is to handle invalid json format from Duke in 1A
        # Read nested json and array as string and handle them separately later
        schema = StructType()
        for field in schema_config:
            # schema = parse_flat_schema_from_config(schema_config[field], field, schema)
            type_config = schema_config[field]
            if isinstance(type_config, str):
                spark_field_type = self.to_spark_types(type_config)
                schema.add(StructField(field, spark_field_type, True))
            else:
                schema.add(field, StringType(), True)
        return schema

    def read_raw_dim_source(self, source):
        source = source.lower()
        version = self.RUN_VERSION
        source_path = [f for f in self.get_raw_source_file_list(source, version, "info")]
        if not any(source_path):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_path[0])))

        # # To be used in ENV 2
        # source_schema = self.build_duke_data_schema(source)
        # spark = self.init_spark_session()
        # raw_source_df = spark.read.json(source_path, source_schema)

        # For ENV 1A
        spark = self.init_spark_session()
        # Read the data as string without, This is done because of the invalid data format
        raw_source_df = spark.read.text(source_path)

        # Replace all '//' with '/' to make the data a valid json
        raw_source_df = raw_source_df.withColumn('data', F.regexp_replace("value", r'(?<!(\\))\\\\\"', '\\\\\"'))
        schema_config = self.SYSTEM_DICT["info_tables"][source]["amc_schema"]
        # Create flat schema where nested jsons and arrays are treated as strings,
        # this is done because of the raw data format in Duke 1A
        raw_data_initial_schema = self.build_flat_data_schema(schema_config)
        # Convert the JSON string column to a DataFrame using the defined schema
        # Data will be a single column with a complex structure based on the schema
        raw_source_df = raw_source_df.withColumn("data", F.from_json("data", raw_data_initial_schema))
        # Now the single column from json is exploded to multiple columns
        cols = []
        for col in schema_config:
            cols.append(f"data.{col}")
        raw_source_df = raw_source_df.select(cols)

        # Convert type of nested jsons and arrays from String to corresponding schema
        for column in schema_config:
            if not isinstance(schema_config[column], str):
                mapped_codes_schema = self.parse_type_from_config(schema_config[column], None)
                raw_source_df = raw_source_df.withColumn(column, F.from_json(column, mapped_codes_schema))
        raw_source_df = raw_source_df.withColumn("FILE_ID", F.split(
            F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
        return raw_source_df