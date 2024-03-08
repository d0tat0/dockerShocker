import os
import json

from pyspark.sql import *
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, LongType, \
    StringType, StructField, StructType, ArrayType, MapType
from pyspark.sql import functions as F

from spark_jobs.spark_job import SparkJob


class SchemaManager(SparkJob):

    def __init__(self, config_file = None):
        super().__init__(config_file)
        self.spark2schema_types = {
            "INT": "INTEGER",
            "BIGINT": "LONG",
            "STRING": "STRING",
            "DOUBLE": "DOUBLE",
            "DATE": "DATE",
            "BOOLEAN": "BOOLEAN"
        }

    def to_spark_types(self, field_type: str):
        type_dict = {
            "INTEGER": IntegerType(),
            "TIMESTAMP": StringType(),
            "LONG": LongType(),
            "TEXT": StringType(),
            "JSON": StringType(),
            "STRING": StringType(),
            "DOUBLE": DoubleType(),
            "DATE": DateType(),
            "BOOLEAN": BooleanType(),
            "ARRAY[STRING]": ArrayType(StringType()),
            "ARRAY[DICT[STRING:STRING]]": ArrayType(MapType(StringType(), StringType()))
        }
        return type_dict.get(field_type, StringType())

    def read_headers(self, file_path: str):
        header_list = []
        type_list = []

        if file_path.endswith(".json"):
            with self.open_file(file_path) as f:
                for row in f:
                    data = json.loads(row)
                    header_list = list(data.keys())
                    type_list = list(data.keys())
                    break
        else:
            with self.open_file(file_path) as f:
                header_list = f.readline().strip().split(self.csv_separator)
                type_list = f.readline().strip().split(self.csv_separator)
        return header_list, type_list

    def build_source_amc_schema(self, source, sample_file, version):
        header_list, _ = self.read_headers(sample_file)
        _, nfer_schema, type_schema = self.read_schema(source)
        overrides = self.load_hash_overrides(source)
        schema = StructType()
        for field in header_list:
            if field in overrides:
                field_type = overrides[field]
            elif field in nfer_schema:
                field_type = type_schema[nfer_schema.index(field)]
            else:
                field_type = "STRING"
            spark_field_type = self.to_spark_types(field_type)
            #print(f"{field}: {spark_field_type}")
            schema.add(StructField(field, spark_field_type, True))
        return schema

    def load_hash_overrides(self, source):
        overrides = {
            "ORIG_FILE_ID": "INTEGER",
            "ROW_SOURCE_ID": "STRING",
            "PATIENT_AGE_AT_EVENT": "STRING",
            "AGE_OF_ONSET": "STRING"
        }
        hash_config = self.SYSTEM_DICT["hash_tables"]["nfer_hash"].get(source.lower(), {})
        for fields in hash_config.values():
            for field in fields:
                overrides[field] = "STRING"
        return overrides

    def extract_df_schema(self, source_df: DataFrame):
        nfer_schema = []
        type_schema = []
        for field in source_df.schema.fields:
            if field.name == "FILE_ID":
                continue
            nfer_schema.append(field.name)
            type_schema.append(self.spark2schema_types[field.dataType.simpleString().upper()])
        return nfer_schema, type_schema

    def build_final_schema(self, source_path, header_file):
        if header_file is None:
            if isinstance(source_path, list):
                source_dir = os.path.dirname(source_path[0])
                header_file = os.path.join(source_dir, f"header.csv")
            else:
                header_file = os.path.join(source_path, "header.csv")

        name_list, type_list = self.read_headers(header_file)
        schema = self.build_schema(name_list, type_list)
        return schema

    def build_schema(self, name_list, type_list):
        schema = StructType()
        for i, field in enumerate(name_list):
            field_type = type_list[i]
            schema.add(StructField(field, self.to_spark_types(field_type)))
        return schema

    def read_schema(self, source):
        source_dict = self.SYSTEM_DICT['data_tables'].get(source.lower())
        amc_schema, nfer_schema, type_schema, = [source_dict[n] for n in ('amc_schema', 'nfer_schema', 'type_schema') ]
        if type(amc_schema) == dict:
            amc_schema = ["ROW_ID"] + list(amc_schema.keys())
        else:
            amc_schema = ["ROW_ID"] + amc_schema
            amc_schema = [h for h in amc_schema if h not in ["EXTRACT_SOURCE", "DEID_DTM", "DEID_VERSION"]]

        # We want all column names to be in upper case,
        # For Duke, json has lowercase/camelcase  keys and hence amc_schema has to be in the same case
        # Raw data reader handles this and data returned by raw_data_reader already has upper case column names
        # All references later should also have upper case and hence converting case of amc_schema
        amc_schema = [column.upper() for column in amc_schema]

        return amc_schema, nfer_schema, type_schema

    def get_timestamp_fields(self, source):
        _, nfer_schema, type_schema = self.read_schema(source)
        return [nfer_schema[i] for i, t in enumerate(type_schema) if t == "TIMESTAMP"]

    def read_unique_schema(self, source):
        table_type = 'data_tables' if source.lower() in self.SYSTEM_DICT['data_tables'] else 'info_tables'
        return self.SYSTEM_DICT[table_type][source.lower()].get('unique_indices', [])

    def read_extraction_schema(self, source):
        extraction_add_ons = []
        if source in self.extraction_configs["tables"]:
            for extraction_config in self.extraction_configs["tables"][source]["extractions"].values():
                extraction_add_ons.extend(extraction_config["add_ons"])
        return extraction_add_ons

    def read_dim_syn_dk_schema(self, source):
        dim_syn_dk_cols = []
        if "interim_dim_syn_config" in self.SYSTEM_DICT:
            source = source.lower()
            if source in self.SYSTEM_DICT["interim_dim_syn_config"]:
                dim_syn_configs = self.SYSTEM_DICT["interim_dim_syn_config"][source]["dim_syns"]
                for key in dim_syn_configs.keys():
                    id_column_name = dim_syn_configs[key]["id_column"]
                    dim_syn_dk_cols.append(id_column_name)
        return dim_syn_dk_cols

    def read_final_schema(self, source):
        source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
        nfer_schema = source_config["nfer_schema"]
        ees_fields = list(source_config.get("addon_fields", {}).keys())
        extraction_add_ons = self.read_extraction_schema(source)
        dim_syn_dk_cols = self.read_dim_syn_dk_schema(source)
        final_schema = nfer_schema + ees_fields + extraction_add_ons + dim_syn_dk_cols
        return final_schema

    def read_datagen_schema(self, source):
        source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
        nfer_schema = source_config["nfer_schema"]
        ees_fields = list(source_config.get("addon_fields", {}).keys())
        extraction_add_ons = self.read_extraction_schema(source)
        final_schema = nfer_schema + ees_fields + extraction_add_ons
        return final_schema

    def read_fact_map_schema(self):
        nfer_schema = self.SYSTEM_DICT['version_tables']['fact_maps_versioned']['nfer_schema']
        type_schema = self.SYSTEM_DICT['version_tables']['fact_maps_versioned']['type_schema']
        return nfer_schema, type_schema

    def create_headers(self, dir_path, nfer_schema, type_schema):
        header_file_name = os.path.join(dir_path, f'header.csv')
        if type_schema in ["INTEGER", "STRING"]:
            type_schema = [type_schema for _ in nfer_schema]
        nfer_schema_str = self.csv_separator.join(nfer_schema)
        type_schema_str = self.csv_separator.join(type_schema)
        self.write_lines(header_file_name, [nfer_schema_str, type_schema_str])

    def create_empty_dataframe(self, nfer_schema, type_schema) -> DataFrame:
        schema = StructType()
        for i, field in enumerate(nfer_schema):
            field_type = type_schema[i]
            schema.add(StructField(field, self.to_spark_types(field_type)))
        empty_df = self.SPARK_SESSION.createDataFrame([], schema)
        return empty_df

    def flatten_header_list(self, amc_schema, root=None):
        flat_schema = []
        for column in amc_schema:
            if isinstance(amc_schema[column], dict):
                new_root = column if root is None else root + '__' + column
                flat_schema.extend(self.flatten_header_list(amc_schema[column], new_root))
            elif isinstance(amc_schema[column], list):
                new_root = column if root is None else root + '__' + column
                for element in amc_schema[column]:
                    flat_schema.extend(self.flatten_header_list(element, new_root))
            else:
                final_col_name = column if root is None else root + '__' + column
                flat_schema.append(final_col_name)
        return flat_schema

    def add_syn_dk_col(self, source_df: DataFrame, source):
        if "interim_dim_syn_config" in self.SYSTEM_DICT:
            source = source.lower()
            print(f"Looking for {source} in  {self.SYSTEM_DICT['interim_dim_syn_config'].keys()}")
            if source in self.SYSTEM_DICT["interim_dim_syn_config"]:
                dim_syn_configs = self.SYSTEM_DICT["interim_dim_syn_config"][source]["dim_syns"]
                for key in dim_syn_configs.keys():
                    columns_for_interim_concept = dim_syn_configs[key]["columns"]
                    id_column_name = dim_syn_configs[key]["id_column"]

                    """
                    # TODO - Fix this, handle patient_race in a different way
                    # Add RACE_DK to PATIENT_RACE_LOOKUP table and not to PATIENT table and handle from omop in a different way
                    # Remove the following line later
                    if id_column_name == "RACE_DK" and source == "patient":
                        dir_path = os.path.join(self.DIM_SYN_INTERIM_DIR, "PATIENT_RACE_DK_MAP")
                        if os.path.exists(dir_path):
                            patient_race_dk_map = self.read_delta_to_df(dir_path)
                            source_df = source_df.join(F.broadcast(patient_race_dk_map), ["NFER_PID"], "left")
                    """

                    # Do not add column if already present.
                    if id_column_name in source_df.columns:
                        continue

                    na_indices = []
                    for col in columns_for_interim_concept:
                        na_col = col.replace('.', '__')
                        na_indices.append(f"{na_col}_NA")
                        source_df = source_df.withColumn(f"{na_col}_NA", F.col(col))

                    source_df = source_df.na.fill(0, na_indices).na.fill("", na_indices)
                    source_df = source_df.withColumn("HASH_KEY", F.concat_ws("|", F.array(sorted(na_indices))))
                    for col in na_indices:
                        source_df = source_df.drop(col)
                    source_df = source_df.withColumn(id_column_name, F.xxhash64("HASH_KEY"))
                    source_df = source_df.drop("HASH_KEY")
        return source_df
