import fnmatch
import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType, StructField, StructType
from raw_data_reader.base_data_reader import BaseRawDataReader
from string import Template
from core.config_vars import DEFAULT_COUNTER, PIPE_SEP, INCR_COUNTER

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from raw_data_reader.base_data_reader import BaseRawDataReader


class MayoRawDataReader(BaseRawDataReader):

    def __init__(self, data_reader, config_file=None):
        super().__init__(data_reader, config_file)

    def read_raw_data(self, source, source_path=None, version=None):
        version = self.data_reader_obj.RUN_VERSION if version is None else version
        if not source_path:
            source_path = self.data_reader_obj.get_raw_source_file_list(source, version, "data")
        source_list = []
        for x in source_path:
            source_list.extend(x[1])
        if not any(source_list):
            return None
        print("Reading {} files from {}".format(len(source_path), os.path.dirname(source_list[0])))
        source_schema = self.build_mayo_data_schema(source, source_path[0][1][0], version)
        if source_schema is None:
            return None
        parse_mode = "FAILFAST" if source not in ["FACT_DEVICEMANUFACTURER"] else "PERMISSIVE"
        raw_source_df = None
        for idx, _source_list in source_path:
            file_type = "csv"
            if isinstance(_source_list, str):
                _source_list = [_source_list]
            for _file in _source_list:
                if _file.endswith(".json"):
                    file_type = "json"
                    break
            if file_type == "csv":
                _source_df = self.data_reader_obj.read_csv_to_df(_source_list, source_schema, parse_mode=parse_mode)
            elif file_type == "json":
                _source_df = self.data_reader_obj.read_json_to_df(_source_list)
                for _struct in source_schema:
                    col_name = _struct.name
                    if _struct.dataType != StringType():
                        _source_df = _source_df.withColumn(col_name, F.col(col_name).cast(_struct.dataType))
            else:
                raise ValueError("Invalid File Type")
            _source_df = _source_df.withColumn("FILE_ID", F.split(
                F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
            _source_df = _source_df.withColumn(INCR_COUNTER, F.lit(idx))
            if not raw_source_df:
                raw_source_df = _source_df
            else:
                raw_source_df = raw_source_df.unionByName(_source_df)
        return raw_source_df

    def build_mayo_data_schema(self, source, sample_file, version):
        header_list, _ = self.data_reader_obj.read_headers(sample_file)
        if not header_list:
            print('No headers in raw data')
            return None
        _, nfer_schema, type_schema = self.data_reader_obj.read_schema(source)
        overrides = self.data_reader_obj.load_hash_overrides(source)
        schema = StructType()
        for field in header_list:
            if field in overrides:
                field_type = overrides[field]
            elif field in nfer_schema:
                field_type = type_schema[nfer_schema.index(field)]
            else:
                field_type = "STRING"
            spark_field_type = self.data_reader_obj.to_spark_types(field_type)
            # print(f"{field}: {spark_field_type}")
            schema.add(StructField(field, spark_field_type, True))
        return schema

    def read_raw_dim_source(self, dim_source, version=None, dim_filename=None):
        version = self.data_reader_obj.RUN_VERSION if version is None else version

        # if filename is given, identify the dim_source
        if dim_filename is not None:
            for _dim_source in self.data_reader_obj.SYSTEM_DICT["info_tables"]:
                config = self.data_reader_obj.SYSTEM_DICT["info_tables"][_dim_source.lower()]
                if dim_filename.upper() == config["files"][0].split('.')[0]:
                    dim_source = _dim_source
                    break

        dim_config = self.data_reader_obj.SYSTEM_DICT["info_tables"][dim_source.lower()]
        dim_folder_name = dim_config['incr_dir_name']
        # source_file_pattern = f"*/{dim_folder_name}/*.txt"
        file_patterns = self.data_reader_obj.SYSTEM_DICT["amc_config"]["raw_info_file_pattern"]
        if isinstance(file_patterns, str):
            file_patterns = [file_patterns]
        source_file_patterns = [Template(i).substitute(source=dim_folder_name) for i in file_patterns]

        dim_df: DataFrame = None
        dim_file_count = 0
        data_file_list_file = os.path.join(self.data_reader_obj.RUN_STATE_DIR, version, "dim_files.txt")
        for line in self.data_reader_obj.open_file(data_file_list_file):
            line = line.strip().split(PIPE_SEP)
            file_path = line[0]
            idx = DEFAULT_COUNTER if len(line) < 2 else int(line[1])
            for _pattern in source_file_patterns:
                if fnmatch.fnmatch(file_path, _pattern):
                    dim_file_count += 1
                    source_file_name = file_path
                    dim_source_df_part = self.data_reader_obj.read_csv_to_df(source_file_name)
                    dim_source_df_part.withColumn(INCR_COUNTER, F.lit(idx))
                    dim_df = dim_df.unionByName(dim_source_df_part) if dim_df else dim_source_df_part
        print("Scanning {0}, found {1} files".format(str(source_file_patterns), dim_file_count))
        return dim_df
