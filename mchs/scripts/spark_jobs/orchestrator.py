from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, LongType, StringType
from delta import *

from core.config_vars import DEFAULT_INVALID_DATE
from spark_jobs.data_reader import DataReader
from spark_jobs.data_writer import DataWriter
import os
from raw_data_reader.mayo_raw_data_reader import MayoRawDataReader
from raw_data_reader.duke_2_raw_data_reader import Duke2RawDataReader
from raw_data_reader.duke_1a_raw_data_reader import Duke1ARawDataReader
from raw_data_reader.mercy_raw_data_reader import MercyRawDataReader



class Orchestrator(DataReader, DataWriter):
    def __init__(self, config_file = None):
        super().__init__(config_file)
        self.print_job_info()
        self.set_raw_data_reader()

    def print_job_info(self):
        print("="*120)
        print(f"DATA_ENV: {self.DATA_ENV}")
        print(f"RELATIVE_DTM: {self.RELATIVE_DTM}")
        print(f"RUN_VERSION: {self.RUN_VERSION}")
        print(f"RUN_MODE: {self.RUN_MODE}")
        print(f"ROOT_DIR: {self.ROOT_DIR}")
        if self.options.debug:
            print(f"DEBUG: {self.options.debug}")
        print(f"LOCAL_DATA_DIR: {self.LOCAL_DATA_DIR if self.LOCAL_DATA_DIR else 'Not Set'}")
        print(f"RUNNING {self.__class__.__name__}.{self.options.run}")
        print("="*120)

    def ees_full_update(self, version=None):
        if version is None:
            version = self.RUN_VERSION
        return float(version) * 1000 % 2 == 0 and float(version) != float(self.SOURCE_DATA_VERSION)

    @staticmethod
    def add_nfer_hash_column(df: DataFrame, column, out_field):
        df = df.withColumn(out_field, F.xxhash64(column))
        # if a value is negative make it positive
        df = df.withColumn(out_field, F.when(F.col(out_field) < 0, F.col(out_field) * -1).otherwise(F.col(out_field)))
        return df

    def calc_nfer_age_for_nfer_dtm(self, source_df: DataFrame, use_old_formula=False):
        if use_old_formula:
            formula = F.year(F.from_unixtime("NFER_DTM")) - F.year(source_df[self.BIRTH_DATE])
        else:
            formula = ((F.col("NFER_DTM") - F.unix_timestamp(F.col(self.BIRTH_DATE), self.BIRTH_DATE_FORMAT)) / (365 * 24 * 60 * 60))
        return source_df.withColumn("NFER_AGE", formula.cast(IntegerType()))

    def de_dupe_patient_meta(self, source_df: DataFrame):
        if not self.PATIENT_MERGED_FLAG:
            return source_df
        window = Window.partitionBy("NFER_PID").orderBy(self.PATIENT_MERGED_FLAG)
        source_df = source_df.withColumn("ROW_NUM", F.row_number().over(window))
        source_df = source_df.filter(F.col("ROW_NUM") == 1)
        source_df = source_df.drop("ROW_NUM")
        return source_df

    def calc_key_hash(self, source_df: DataFrame, unique_indices, field="KEY_HASH", debug=False) -> DataFrame:
        na_indices = []
        for col in unique_indices:
            # Handling JSON column that are part of unique indices
            # We have to explicitly get the json key column to create hashes
            na_col = col.replace('.', '__')
            na_indices.append(f"{na_col}_NA")
            source_df = source_df.withColumn(f"{na_col}_NA", F.col(col))
        source_df = source_df.na.fill(0, na_indices).na.fill("", na_indices)
        source_df = source_df.withColumn(f"{field}_STR", F.concat_ws("|", F.array(sorted(na_indices))))
        source_df = source_df.withColumn(field, F.sha2(F.col(f"{field}_STR"), 256))
        for col in na_indices:
            source_df = source_df.drop(col)
        if not debug:
            source_df = source_df.drop(f"{field}_STR")
        return source_df

    def pack_fact_guid(self, source_df: DataFrame, fields=[]) -> DataFrame:
        if not fields:
            fields = ["FACT_TABLE_ID", "PDK_ID", "FACT_KEY_ID"]
        source_df = source_df.withColumn("FACT_GUID", F.lit(0).cast("long"))
        source_df = source_df.withColumn("FACT_GUID", F.col("FACT_GUID") + F.shiftleft(F.col(fields[0]).cast("long"), 56))
        source_df = source_df.withColumn("FACT_GUID", F.col("FACT_GUID") + F.shiftleft(F.col(fields[1]).cast("long"), 28))
        source_df = source_df.withColumn("FACT_GUID", F.col("FACT_GUID") + F.col(fields[2]).cast("long"))
        return source_df

    def pack_hash_guid(self, source_df: DataFrame, field) -> DataFrame:
        guid_field = f"{field}_GUID"
        source_df = source_df.withColumn(guid_field, F.lit(0).cast("long"))
        source_df = source_df.withColumn(guid_field, F.col(guid_field) + F.shiftleft(F.col("NFER_PID").cast("long"), 32))
        source_df = source_df.withColumn(guid_field, F.col(guid_field) + F.col(field).cast("long")).drop(field)
        source_df = source_df.withColumnRenamed(guid_field, field)
        return source_df

    def unpack_fact_guid(self, source_df: DataFrame, columns=[]) -> DataFrame:
        source_df = source_df.withColumn("FACT_GUID_BINARY", F.lpad(F.bin("FACT_GUID").cast(StringType()), 64, "0"))
        source_df = source_df.withColumn("FACT_TABLE_ID", F.conv(F.substring("FACT_GUID_BINARY", 1, 8), 2, 10).cast(IntegerType()))
        source_df = source_df.withColumn("PDK_ID", F.conv(F.substring("FACT_GUID_BINARY", 9, 28), 2, 10).cast(IntegerType()))
        source_df = source_df.withColumn("FACT_KEY_ID", F.conv(F.substring("FACT_GUID_BINARY", 37, 28), 2, 10).cast(IntegerType()))
        source_df = source_df.drop("FACT_GUID_BINARY")

        if columns:
            drop_cols = [col for col in ["FACT_TABLE_ID", "PDK_ID", "FACT_KEY_ID"] if col not in columns]
            source_df = source_df.drop(*drop_cols)

        return source_df

    def get_norm_patient_type(self, source_df:DataFrame, field):
        source_df = source_df.withColumn("PATIENT_TYPE", F.lower(F.col(field)))
        source_df = source_df.withColumn("PATIENT_TYPE",
            F.when(F.col("PATIENT_TYPE").contains("inpatient"), "inpatient")
            .when(F.col("PATIENT_TYPE").isin("ip transitional care", "ip behavioral health", "ip rehab", "ip hospice"), "inpatient")
            .when(F.col("PATIENT_TYPE").contains("op in bed"), "overnight bed")
            .when(F.col("PATIENT_TYPE").contains("op bed"), "overnight bed")
            .when(F.col("PATIENT_TYPE").contains("overnight"), "overnight bed")
            .when(F.col("PATIENT_TYPE").contains("emergency"), "emergency")
            .otherwise(F.lit("outpatient"))
        )
        return source_df

    def get_normalized_site(self, source_df:DataFrame, field):
        source_df = source_df.withColumn("SITE", F.upper(field))
        source_df = source_df.withColumn("SITE",
            F.when(F.col("SITE").isin("MCHS"), "MCHS")
            .when(F.col("SITE").isin("RST"), "Rochester")
            .when(F.col("SITE").isin("FLA"), "Florida")
            .when(F.col("SITE").isin("ARZ"), "Arizona")
            .otherwise(F.lit("Others"))
        )
        return source_df

    def normalize_ees_field(self, source_df:DataFrame, field):
        source_df = source_df.withColumn(field, F.regexp_replace(F.col(field), " nferdelimnfer ", ","))
        source_df = source_df.withColumn(field, F.concat_ws(",", F.sort_array(F.split(F.col(field), ","))))
        return source_df
    """
    If nferdelimnfer is in the string, replace it with comma - for fact diagnosis only
    """
    def normalize_extraction_field(self, source_df:DataFrame, field, delim):
        source_df = source_df.withColumn(field, F.regexp_replace(F.col(field), " nferdelimnfer ", "##"))
        source_df = source_df.withColumn(field, F.regexp_replace(F.col(field), "\|", "-"))
        source_df = source_df.withColumn(field, F.concat_ws(delim, F.sort_array(F.split(F.col(field), "##"))))
        return source_df

    def parse_timestamp(self, source_df: DataFrame, field, custom_dtm_format=None, use_default=True):
        if self.RELATIVE_DTM:
            source_df = source_df.withColumn(field, F.col(field) + F.col(self.ENCOUNTER_EPOCH))
        else:
            if self.DTM_FORMAT and use_default:
                source_df = source_df.withColumn(field, F.unix_timestamp(source_df[field], self.DTM_FORMAT))
            elif custom_dtm_format: # To enhance performance in case dtm_format is not default but known.
                source_df = source_df.withColumn(field, F.unix_timestamp(source_df[field], custom_dtm_format))
            else:
                source_df = source_df.withColumn(field,
                                                 F.when(source_df[field].rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$"),
                                                        F.unix_timestamp(source_df[field], "yyyy-MM-dd'T'HH:mm:ssXXX"))
                                                 .when(source_df[field].rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$"),
                                                       F.unix_timestamp(source_df[field], "yyyy-MM-dd'T'HH:mm:ss"))
                                                 .when(source_df[field].rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$"),
                                                       F.unix_timestamp(source_df[field], "yyyy-MM-dd HH:mm:ss"))
                                                 .when(source_df[field].rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
                                                       F.unix_timestamp(source_df[field], "yyyy-MM-dd"))
                                                 .otherwise(None))
        return source_df

    def parse_dtm_str(self, source_df:DataFrame, field):
        date_formats = [
            (29, "%Y-%m-%d %H:%M:%S.%f %p"),
            (26, "%Y-%m-%d %H:%M:%S.%f"),
            (19, "%Y-%m-%d %H:%M:%S"),
            (10, "%Y-%m-%d")
        ]
        date_format_df = self.SPARK_SESSION.createDataFrame(date_formats, ["LENGTH", "FORMAT"])
        source_df = source_df.join(F.broadcast(date_format_df), F.length(field) == F.col("LENGTH"), "left")
        source_df = source_df.withColumn(field,
            F.when(F.col(field).cast(LongType()).isNotNull(),
                F.col(field).cast(LongType()) + F.coalesce(self.ENCOUNTER_EPOCH, F.lit(0)))
            .otherwise(F.unix_timestamp(source_df[field], F.coalesce("FORMAT", "%Y-%m-%d")))
        )
        source_df = source_df.withColumn(field, F.coalesce(F.col(field), F.lit(DEFAULT_INVALID_DATE)))
        return source_df

    def parse_options_source(self):
        data_table_dict = self.SYSTEM_DICT['data_tables']
        table_list = [t.upper() for t in data_table_dict if data_table_dict[t].get("is_event_table")]
        filter_set = set(table_list)
        if not self.options.source:
            return
        if self.options.source == 'ALL':
            return self.filter_tables(table_list, filter_set)
        if self.options.source == 'BIG3':
            filter_set = filter_set.intersection(set(self.BIG_TABLES))
            return self.filter_tables(table_list, filter_set)
        elif self.options.source == 'NOTBIG3':
            filter_set = filter_set - set(self.BIG_TABLES)
            return self.filter_tables(table_list, filter_set)
        elif self.options.source == 'EXTRACTIONS':
            filter_set = filter_set.intersection(set(self.SYSTEM_DICT["extraction_jobs"]["tables"].keys()))
            return self.filter_tables(table_list, filter_set)
        elif self.options.source == 'EXTRACTIONS_DRUG_UPDATE':
            src_lst = ['FACT_ORDERS','FACT_MED_INFUSION','FACT_MEDS_ADMINISTERED']
            return src_lst
        elif self.options.source == 'BRIDGE_TABLES':
            return ["DIM_PATHOLOGY_DIAGNOSIS_CODE_BRIDGE", "DIM_SURGICAL_CASE_NOTE_BRIDGE","DIM_APPOINTMENT_INDICATION_BRIDGE"]
        else:
            # a list of comma separated table names
            if "," in self.options.source or "~" in self.options.source:
                if "~" in self.options.source:
                    table_list = list(set(self.filter_tables(table_list, filter_set)) - set(self.options.source.replace("~", "").split(',')))
                else:
                    table_list = self.options.source.split(',')
                return [table.strip().upper() for table in table_list]
            return [self.options.source.upper()]

    def parse_options_version_list(self):
        if not self.options.version_list:
            return []
        return self.options.version_list.split(',')

    def filter_tables(self, table_list, filter_set):
        skip_table_list = ['FACT_GENOMIC_BIOSPECIMEN_FLAGS']
        final_table_list = [t for t in table_list if t in filter_set and t not in skip_table_list]
        small_table_list = [t for t in final_table_list if t not in self.BIG_TABLES]
        big_table_list = [t for t in self.BIG_TABLES if t in final_table_list]
        return sorted(small_table_list) + big_table_list

    def parse_options_dim_source(self):
        dim_table_dict = self.SYSTEM_DICT['info_tables']
        dim_tables = sorted([t.upper() for t in dim_table_dict])
        sources = sorted(list(set(self.options.source.upper().split(",")).intersection(set(dim_tables))))
        if not self.options.source:
            return
        elif sources:
            return sources
        return dim_tables

    def locate_harmonization_dir(self, table, skip_local_dir=False):
        harmonized_dir_name = os.path.join(self.harmonized_root_dir, table)
        dirs_to_check_in_order = [self.DATA_DIR, self.LOCAL_DATA_DIR, self.NFERX_DATA_DIR]
        if skip_local_dir:
            dirs_to_check_in_order = [self.DATA_DIR, self.NFERX_DATA_DIR]

        """
        
        for dir_path in dirs_to_check_in_order:
              = os.path.join(dir_path, harmonized_dir_name, "*", "HARMONIZED_META", "orchestrator_meta.json")
            print("Checking for harmonized data in:", latest_meta_file)
            if self.glob(latest_meta_file):
                break
        """

        _folder_path = None
        print(f'dirs_to_check_in_order: {dirs_to_check_in_order}')
        for orch_version in dirs_to_check_in_order:
            if self.glob(os.path.join(orch_version, harmonized_dir_name)):
                _folder_path = os.path.join(orch_version, harmonized_dir_name)
                break
        # latest_folder_path = self.get_latest_folder(input)
        if self.SAMPLE_RUN and not _folder_path:
            return None, {}
        print(f' _folder_path: {_folder_path}')
        print(f' self.harmonized_meta_dir: {self.harmonized_meta_dir}')
        meta_file = os.path.join(_folder_path, self.harmonized_meta_dir, "orchestrator_meta.json")
        if not meta_file:
            raise Exception(f"Could not find harmonized data for {table}")
        latest_meta = self.get_json_data(meta_file)
        print("Harmonization Meta:", latest_meta)
        print("Harmonization Dir:", _folder_path)
        return _folder_path, latest_meta

    def set_raw_data_reader(self):
        if self.DATA_ENV in ["MAYO_CDAP"]:
            self.raw_data_reader = MayoRawDataReader(self, self.config_file)
        elif self.DATA_ENV in ["MERCY"]:
            self.raw_data_reader = MercyRawDataReader(self, self.config_file)
        elif self.DATA_ENV in ["DUKE_1A"]:
            self.raw_data_reader = Duke1ARawDataReader(self, self.config_file)
        elif self.DATA_ENV in ["DUKE_2"]:
            self.raw_data_reader = Duke2RawDataReader(self, self.config_file)
