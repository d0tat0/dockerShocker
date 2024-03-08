import fnmatch
import os
from string import Template

from pyspark.sql import *
from pyspark.sql import functions as F

from spark_jobs.schema_manager import SchemaManager
from core.config_vars import PIPE_SEP, RunMode, DEFAULT_COUNTER, INCR_COUNTER


class DataReader(SchemaManager):

    def __init__(self, config_file = None):
        super().__init__(config_file)

    def read_csv_to_df(self, file_path, schema=None, header=True, sep=None, parse_mode="FAILFAST") -> DataFrame:
        infer_schema = False if schema else True
        sep = sep if sep else self.csv_separator
        return self.SPARK_SESSION.read.csv(
            file_path,
            header=header,
            schema=schema,
            inferSchema=infer_schema,
            sep=sep,
            mode=parse_mode,
            quote=""
        )

    def read_parquet_to_df(self, file_path) -> DataFrame:
        print(f"Reading {file_path}")
        if self.glob(file_path):
            return self.SPARK_SESSION.read.parquet(file_path)

    def read_json_to_df(self, file_path, schema=None) -> DataFrame:
        print(f"Reading {file_path}")
        return self.SPARK_SESSION.read.json(file_path, schema=schema)

    def read_delta_to_df(self, file_path) -> DataFrame:
        print(f"Reading {file_path}")
        source_df = self.SPARK_SESSION.read.format("delta").load(file_path)
        return source_df

    def read_source_files(self, source, version=None) -> DataFrame:
        version = version if version else self.RUN_VERSION
        source_dir = os.path.join(self.ROOT_SOURCE_DIR, version, source)
        if not self.glob(source_dir):
            return None
        source_df = self.read_parquet_to_df(source_dir)
        return source_df

    def read_final_dir(self, source_path, header_file=None, sep=None) -> DataFrame:
        if not isinstance(source_path, list):
            if self.glob(os.path.join(source_path, "*.parquet")):
                return self.read_parquet_to_df(source_path)
            source_dir = source_path
            print("Reading {}".format(source_dir))
            source_path = os.path.join(source_path, "patient_*.final")
        else:
            source_dir = os.path.dirname(source_path[0])
            print("Reading {0} files from {1}".format(len(source_path), source_dir))
        source_schema = self.build_final_schema(source_dir, header_file)
        source_df = self.read_csv_to_df(source_path, source_schema, header=False, sep=sep)
        source_df = source_df.withColumn("FILE_ID", F.split(F.reverse(F.split(F.input_file_name(), "_"))[0], "\\.")[0].cast("INT"))
        return source_df

    def past_version_filter(self, source_df: DataFrame, version, latest_only=True) -> DataFrame:
        version = round(float(version),3)
        source_df = source_df.filter(F.col("VERSION") <= F.lit(version))
        source_df = source_df.withColumn("UPDATED_BY",
            F.when(F.col("UPDATED_BY") > version, F.lit(0.0)).otherwise(F.col("UPDATED_BY")))
        if latest_only:
            source_df = source_df.filter(F.col("UPDATED_BY") == F.lit(0.0))
        return source_df

    def read_versioned_dim_maps(self, columns=[], latest_only=True, version=None):
        dim_maps_dir = self.latest_data_dir("DATAGEN", "DIM_MAPS_VERSIONED")
        dim_maps_version = self.latest_data_version("DATAGEN", "DIM_MAPS_VERSIONED")
        dim_maps_df = self.read_final_dir(dim_maps_dir)
        if version and float(version) < dim_maps_version:
            print("Applying past version filter.")
            dim_maps_df = self.past_version_filter(dim_maps_df, version, latest_only)
        elif latest_only:
            dim_maps_df = dim_maps_df.filter(F.col("UPDATED_BY") == F.lit(0.0))
        if columns:
            dim_maps_df = dim_maps_df.select(columns)
        return dim_maps_df

    def read_versioned_datagen_table(self, source, version, columns=[], filters=None, latest_only=True):
        #print('read_versioned_datagen_table', source, version, filters, latest_only)
        try:
            read_version = float(version)
            latest_delta_table_dir = self.latest_delta_dir("DELTA_TABLES", "FACT_TABLES", f"{source.upper()}.parquet")
            if not latest_delta_table_dir:
                return

            latest_parquet_version = self.latest_delta_version("DELTA_TABLES", "FACT_TABLES", f"{source.upper()}.parquet")
            latest_parquet_version = float(latest_parquet_version)
            
            source_df = self.read_delta_to_df(latest_delta_table_dir)
            if read_version < latest_parquet_version:
                print(f"Applying past-version-filter={version}")
                source_df = self.past_version_filter(source_df, version, latest_only=False)

            if latest_only:
                source_df = source_df.filter(F.col("UPDATED_BY") == F.lit(0.0))

            if not columns or "FILE_ID" in columns:
                source_df = self.assign_file_id_for_nfer_pid(source_df, source)

            if filters:
                for key in filters:
                    source_df = source_df.filter(source_df[key].isin(filters[key]))

            syn_dk_cols = self.read_dim_syn_dk_schema(source)
            if columns:
                if set(columns).intersection(set(syn_dk_cols)):
                    source_df = self.add_syn_dk_col(source_df, source)
            else:
                source_df = self.add_syn_dk_col(source_df, source)
            if columns:
                source_df = source_df.select(columns)
            return source_df
        except Exception as e:
            print(f"WARN: Failed to read {version}/{source}.parquet: {e}")

    def create_empty_dataframe_for_source(self, source, columns=None, amc_schema=False):
        amc_sch, nfer_schema, type_schema = self.read_schema(source)
        if amc_schema:
            return self.create_empty_dataframe_for_source(amc_schema, ["STRING" for i in amc_sch])
        nfer_schema += ["FILE_ID"]
        type_schema += ["INTEGER"]
        if columns:
            final_nfer_schema = []
            final_type_schema = []
            for col in columns:
                idx = nfer_schema.index(col)
                final_nfer_schema.append(col)
                final_type_schema.append(type_schema[idx])
            nfer_schema = final_nfer_schema
            type_schema = final_type_schema
        return self.create_empty_dataframe(nfer_schema, type_schema)

    def read_versioned_datagen_dir(self, source, columns=[], filters={}, latest_only=True, version=None, force_csv_read=False) -> DataFrame:
        #print('read_versioned_datagen_dir', source, filters, latest_only, version, force_csv_read)
        """
        Reads the versioned directory for the given source.
        :param source: Source name
        :param columns: List of columns to read
        :param latest_only: If true, only the latest version of the source is read
        :param version: If not None, only the given version of the source is read
        :param filters: Dictionary of filter in the form of {column: [value1,value2]}
        :return: DataFrame
        """
        if version is None:
            version = self.RUN_VERSION

        source_df = None
        latest_version = self.latest_delta_version("DELTA_TABLES", "FACT_TABLES", f"{source.upper()}.parquet")
        final_df = self.read_versioned_datagen_table(source, version, columns=columns, filters=filters, latest_only=latest_only)
        if not force_csv_read or float(latest_version) >= float(version):
            if final_df and all([col in final_df.columns for col in columns]):
                return final_df

        last_version = None
        schema = None
        for run_version, run_meta in self.RUN_STATE_LOG.items():
            if float(run_version) <= float(latest_version):
                continue
            final_dir_path = self.data_dir("DATAGEN", source.upper(), run_version=run_version)
            if not final_dir_path:
                continue
            run_version_df = self.read_final_dir(final_dir_path)
            if "FILE_ID" not in run_version_df.columns:
                run_version_df = self.assign_file_id_for_nfer_pid(run_version_df, source)
                run_version_df = run_version_df.drop("DIM_FILE_ID")
            if filters:
                for key in filters:
                    run_version_df = run_version_df.filter(run_version_df[key].isin(filters[key]))
            if schema is None:
                schema = run_version_df.columns
            run_version_df = run_version_df.select(schema)
            source_df = run_version_df if source_df is None else source_df.union(run_version_df)
            last_version = run_version
            if run_version == version:
                break

        if source_df is None:
            print(f"Failed to read {source} version {version}")
            if self.SAMPLE_RUN:
                return self.create_empty_dataframe_for_source(source, columns)
            return

        # For data that is read from DATAGEN DIR, SYN_DK columns will be absent
        source_df = self.add_syn_dk_col(source_df, source)

        if columns:
            source_df = self.safe_select(source_df, columns, ["NFER_PID", "ROW_ID"])

        version_columns = ["NFER_PID", "ROW_ID", "VERSION", "UPDATED_BY"]
        guid_col = "FACT_GUID"

        if source == self.DIM_PATIENT:
            guid_col = "NFER_PID"
            version_dir = self.data_dir("DATAGEN", "DIM_MAPS_VERSIONED", run_version=last_version)
            if not version_dir:
                raise Exception(f"Versioned dim maps not found at {last_version}")
            version_df = self.read_final_dir(version_dir).drop(self.BIRTH_DATE).drop(self.ENCOUNTER_EPOCH)
            version_df = self.safe_select(version_df, columns, version_columns).drop("FILE_ID")
        else:
            version_dir = self.data_dir("FACT_MAPS_VERSIONED", source, run_version=last_version)
            if not version_dir:
                raise Exception(f"Versioned fact maps not found at {last_version}")
            version_df = self.read_final_dir(version_dir)
            version_df = self.safe_select(version_df, columns, version_columns).drop("FILE_ID")

        source_df = source_df.join(version_df, ["NFER_PID", "ROW_ID"], "LEFT")
        change_df = source_df.select([guid_col, "UPDATED_BY"]).withColumn("NEW", F.lit(True))
        final_df = final_df.join(change_df, [guid_col, "UPDATED_BY"], "LEFT")
        final_df = final_df.withColumn("UPDATED_BY", F.when(F.col("NEW") & F.col("UPDATED_BY").isin(0.0),
                                                            F.lit(version)).otherwise(F.col("UPDATED_BY"))).drop("NEW")
        final_df = final_df.unionByName(source_df)
        if latest_only:
            final_df = final_df.filter(F.col("UPDATED_BY").isin(0.0))
        if columns:
            final_df = self.safe_select(final_df, required_columns=columns)

        return final_df

    def safe_select(self, source_df: DataFrame, required_columns, mandatory_columns = []) -> DataFrame:
        req_col_ordered_bkup = required_columns
        mand_col_ordered_bkup = mandatory_columns

        mandatory_columns = list(set(mandatory_columns).intersection(set(source_df.columns)))
        reqired_columns = list(set(required_columns).intersection(set(source_df.columns)))
        all_cols_to_select = mandatory_columns + reqired_columns

        final_list = []
        for col in req_col_ordered_bkup:
            if col in all_cols_to_select:
                final_list.append(col)

        for col in mand_col_ordered_bkup:
            if col in all_cols_to_select and col not in final_list:
                final_list.append(col)

        assert (len(set(all_cols_to_select).union(set(final_list))) == len(set(all_cols_to_select).intersection(set(final_list))))
        return source_df.select(final_list)

    def read_versioned_syn_data(self, syn_source, version=None, latest_only=True):
        if version is None:
            version = self.RUN_VERSION

        syn_config = self.SYSTEM_DICT["syn_tables"].get(syn_source)
        if not syn_config:
            raise Exception(f"Syn table {syn_source} not found in syn-tables-config")

        syn_table_name = syn_config["table_name"]
        syn_dir_name = syn_config.get("dir_name", "")
        syn_data_dir = self.latest_data_dir(syn_dir_name)
        syn_data_version = self.latest_data_version(syn_dir_name)
        syn_parquet_dir = self.latest_data_dir("DELTA_TABLES", "SYN_TABLES", f"{syn_table_name}.parquet")
        syn_parquet_version = self.latest_data_version("DELTA_TABLES", "SYN_TABLES", f"{syn_table_name}.parquet")

        syn_data_df = None
        apply_past_version_filter = False

        if syn_parquet_version and float(version) <= syn_parquet_version:
            syn_data_df = self.read_delta_to_df(syn_parquet_dir)
            if float(version) < syn_parquet_version:
                apply_past_version_filter = True

        elif syn_dir_name and syn_data_version and float(version) <= syn_data_version:
            syn_data_df = self.read_final_dir(syn_data_dir)
            if float(version) < syn_data_version:
                apply_past_version_filter = True

        elif syn_parquet_version:
            print(f"WARNING: Syn table {syn_source} not found in {version}. Reading from {syn_parquet_dir}")
            syn_data_df = self.read_delta_to_df(syn_parquet_dir)

        elif syn_dir_name and syn_data_version:
            print(f"WARNING: Syn table {syn_source} not found in {version}. Reading from {syn_data_dir}")
            syn_data_df = self.read_final_dir(syn_data_dir)

        else:
            raise Exception(f"Syn table {syn_source} not found in {syn_data_dir} or {syn_parquet_dir}")

        if apply_past_version_filter:
            syn_data_df = self.past_version_filter(syn_data_df, version, latest_only=latest_only)
        elif latest_only:
            syn_data_df = syn_data_df.filter(F.col("UPDATED_BY").isin(0.0))
        syn_data_df = self.add_syn_dk_col(syn_data_df, syn_source)
        return syn_data_df

    def read_raw_dim_source(self, dim_source):
        dim_config = self.SYSTEM_DICT["info_tables"][dim_source.lower()]
        dim_folder_name = dim_config['incr_dir_name']
        source_file_pattern = f"*/{dim_folder_name}/*.txt"
        dim_df: DataFrame = None
        dim_file_count = 0
        data_file_list_file = os.path.join(self.RUN_STATE_DIR, self.RUN_VERSION, "dim_files.txt")
        for line in self.open_file(data_file_list_file):
            line = line.strip().split(PIPE_SEP)
            file_path = line[0]
            idx = DEFAULT_COUNTER if len(line) < 2 else int(line[1])
            if fnmatch.fnmatch(file_path, source_file_pattern):
                dim_file_count += 1
                source_file_name = file_path
                dim_source_df_part = self.read_csv_to_df(source_file_name)
                dim_source_df_part.withColumn(INCR_COUNTER, F.lit(idx))
                dim_df = dim_df.unionByName(dim_source_df_part) if dim_df else dim_source_df_part
        print("Scanning {0}, found {1} files".format(source_file_pattern, dim_file_count))
        return dim_df

    def read_dim_source(self, dim_source):
        dim_file_name = self.SYSTEM_DICT["info_tables"][dim_source.lower()]["files"][0]
        dim_dir_name, ext = os.path.splitext(dim_file_name)
        dim_file_path = os.path.join(self.INPUT_SOURCE_DIR, "DIM_INFO", dim_dir_name)
        dim_source_df = self.read_parquet_to_df(dim_file_path)
        return dim_source_df

    def read_versioned_dim_table(self, dim_source, latest_only=True):
        dim_file_name = self.SYSTEM_DICT["info_tables"][dim_source.lower()]["files"][0]
        dim_file_path = self.latest_data_dir( "DATAGEN/DIM_INFO", dim_file_name)
        if not dim_file_path or not self.check_file_exists(dim_file_path):
            return None
        print("Reading {}".format(dim_file_path))
        if self.IS_CSV_MODE:
            dim_source_df = self.read_csv_to_df(dim_file_path)
        else:
            dim_source_df = self.read_parquet_to_df(dim_file_path)
        if latest_only:
            dim_source_df = dim_source_df.filter(F.col("UPDATED_BY").isin(0.0))
        return dim_source_df

    def read_last_dim_hash_maps(self, hash_map_dir_name: str, schema=None) -> DataFrame:
        if self.RUN_MODE == RunMode.Full.value and self.OLD_MAPS_DIR:
            hash_map_dir = os.path.join(self.OLD_MAPS_DIR, hash_map_dir_name)
        else:
            hash_map_dir = self.last_data_dir(hash_map_dir_name)

        if not hash_map_dir:
            return None
        if self.RUN_MODE == RunMode.Full.value:
            return self.read_csv_to_df(hash_map_dir, schema=schema)
        return self.read_parquet_to_df(hash_map_dir)

    def read_dim_hash_maps(self, hash_map_dir_name: str) -> DataFrame:
        hash_map_dir = self.latest_data_dir(hash_map_dir_name)
        return self.read_parquet_to_df(hash_map_dir)

    def read_pid_change_maps(self, run_version=None, columns=None) -> DataFrame:
        if run_version is None:
            run_version = self.RUN_VERSION
        pid_change_filename = "DATAGEN/DIM_MAPS/clinic_no_change.txt"
        pid_change_file = os.path.join(self.ROOT_DIR, run_version, pid_change_filename)
        if self.glob(pid_change_file):
            df = self.read_csv_to_df(pid_change_file)
            if columns:
                df = df.select(columns)
            return df
        return None

    def get_raw_source_file_list_old(self, source, version):
        data_file_list_file = os.path.join(self.RUN_STATE_DIR, version, "data_files.txt")
        print(f"Reading data files from {data_file_list_file}")
        source_file_pattern = f"*/{source}/*{source}_[0-9]*.txt"
        source_file_list = {}
        final_source_file_list = []
        for line in self.open_file(data_file_list_file):
            line = line.strip().split(PIPE_SEP)
            file_path = line[0]
            idx = DEFAULT_COUNTER if len(line) < 2 else int(line[1])
            if fnmatch.fnmatch(file_path, source_file_pattern):
                source_file_list.setdefault(idx, []).append(file_path)
        print("Scanning {0}, found {1} files".format(source_file_pattern, sum([len(i) for i in source_file_list.values()])))
        if len(source_file_list) == 0 and self.RUN_MODE == RunMode.Full.value:
            print(f"No files found matching {source_file_pattern}")
        for idx, source_list in sorted(source_file_list.items(), key=lambda x: x[0]):
            final_source_file_list.append((idx, sorted(source_list)))
        return final_source_file_list

    def get_raw_source_file_list(self, source, version, file_type="data"):
        file_name = "data_files.txt" if file_type == "data" else "dim_files.txt"
        data_file_list_file = os.path.join(self.RUN_STATE_DIR, version, file_name)
        print(f"Reading data files from {data_file_list_file}")

        file_pattern = "raw_data_file_pattern" if file_type == "data" else "raw_info_file_pattern"
        source_file_patterns = self.SYSTEM_DICT["amc_config"].get(file_pattern, None)
        if source_file_patterns:
            if isinstance(source_file_patterns, str):
                source_file_patterns = [source_file_patterns]
            file_name_case = self.SYSTEM_DICT["amc_config"]["file_name_case"]
            if file_name_case == "upper":
                source_name = source.upper()
            elif file_name_case == "lower":
                source_name = source.lower()
            else:
                source_name = source
            source_file_patterns = [Template(i).substitute(source=source_name) for i in source_file_patterns]
        else:
            if version.startswith("4."):
                source_file_patterns = [f"*/*{source}_[0-9]*.txt"]
            else:
                source_file_patterns = [f"*/{source}/*{source}_[0-9]*.txt"]

        source_file_list = {}
        final_source_file_list = []
        for line in self.open_file(data_file_list_file):
            line = line.strip().split(PIPE_SEP)
            file_path = line[0]
            idx = DEFAULT_COUNTER if len(line) < 2 else int(line[1])
            for _pattern in source_file_patterns:
                if fnmatch.fnmatch(file_path, _pattern):
                    source_file_list.setdefault(idx, []).append(file_path)

        print("Scanning {0}, found {1} files".format(str(source_file_patterns), len(source_file_list)))
        if len(source_file_list) == 0 and self.RUN_MODE == RunMode.Full.value:
            print(f"No files found matching {str(source_file_patterns)}")

        for idx, source_list in sorted(source_file_list.items(), key=lambda x: x[0]):
            final_source_file_list.append((idx, sorted(source_list)))
        return final_source_file_list

    def read_last_dim_syn(self, table_name):
        data_dir = self.last_data_dir(self.harmonized_root_dir, self.dim_syn_interim_dir_name, self.harmonized_out_dir, table_name.upper())
        if data_dir:
            print(f"Found previous version of data in {data_dir}")
            data_df = self.read_parquet_to_df(data_dir)
            return data_df
        else:
            print(f"Couldn't find previous version of data for {table_name}")
            return None

    def read_latest_dim_syn(self, table_name):
        data_dir = self.latest_data_dir(self.harmonized_root_dir, self.dim_syn_interim_dir_name, self.harmonized_out_dir, table_name.upper())
        if data_dir:
            print(f"Found latest version of data in {data_dir}")
            data_df = self.read_parquet_to_df(data_dir)
            return data_df
        else:
            raise Exception(f"Couldn't find latest version of data for {table_name}")

    def read_incremental_raw_data(self, source, nfer_schema=None):
        source_df = self.read_source_files(source)
        if not source_df:
            return None
        dim_maps_df = self.read_versioned_dim_maps(columns=[self.PATIENT_DK, "NFER_PID"])
        source_df = source_df.join(F.broadcast(dim_maps_df), [self.PATIENT_DK], "LEFT")
        source_df = source_df.na.fill(0, subset=["NFER_PID"])
        # ADD SYN_DK_COLUMNS if any
        source_df = self.add_syn_dk_col(source_df, source)

        if nfer_schema:
            source_df = source_df.select(nfer_schema)
        return source_df
