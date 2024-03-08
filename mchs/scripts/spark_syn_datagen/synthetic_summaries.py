import os
import time
import glob
from collections import Counter
from pyspark import StorageLevel

from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, MapType

from core.config_vars import TAB_SEP, RunMode
from spark_jobs.orchestrator import Orchestrator

ONE_GB = 1024 * 1024 * 1024



class SynDataGenJob(Orchestrator):
    def __init__(self):
        super().__init__()
        self.parent = self

    def run(self):
        if 'delta_table' in self.options.run:
            self.gen_spark_sql_delta_tables()
        if 'parquet2final' in self.options.run:
            self.parquet_to_final()

        # TO DO: MOVE TO NEW JOB
        if 'harmonized_vars' in self.options.run:
            self.format_harmonized_variables()
        if 'infox_data_sql' in self.options.run:
            self.generate_infox_data()
        if 'infohyc_data_full' in self.options.run:
            self.generate_infox_disease_association_dfs_for_hyc()
            self.generate_infox_final_df_for_hyc()
            self.generate_infox_patient_blocks_for_hyc()
        if 'infohyc_partial' in self.options.run:
            # self.generate_infox_disease_association_dfs_for_hyc()
            # self.generate_infox_final_df_for_hyc()
            self.generate_infox_patient_blocks_for_hyc()
    @staticmethod
    def calc_key_hash(source_df: DataFrame, unique_indices, field="KEY_HASH", debug=False) -> DataFrame:
        na_indices = []
        for col in unique_indices:
            na_indices.append(f"{col}_NA")
            source_df = source_df.withColumn(f"{col}_NA", F.col(col).cast(StringType()))
        source_df = source_df.na.fill(0, na_indices).na.fill("", na_indices)
        source_df = source_df.withColumn(f"{field}_STR", F.concat_ws("|", F.array(sorted(na_indices))))
        source_df = source_df.withColumn(field, F.sha2(F.col(f"{field}_STR"), 256))
        for col in na_indices:
            source_df = source_df.drop(col)
        if not debug:
            source_df = source_df.drop(f"{field}_STR")
        return source_df

    def write_versioned_syn_data(self, syn_config, syn_data_df: DataFrame, extra_counters={}, version=None,
                                 is_base_version=False, tag=None):
        # GENERATE DELTA UPDATES
        if version is None:
            version = self.RUN_VERSION
            source_list = syn_config.get("source_list", [])
            if source_list and not self.check_for_source_updates(source_list):
                print("No Source updates available for {}".format(",".join(source_list)))
                return

        if 'NFER_PID' in syn_data_df.columns:
            syn_data_df = syn_data_df.filter(syn_data_df['NFER_PID'] != 0)


        syn_data_df.cache()
        # validates primary key
        syn_data_count = self.validate_syn_data(syn_config, syn_data_df)
        if syn_data_count == 0:
            print("No data for {}".format(syn_config['dir_name']))
            return

        source_dir = syn_config["dir_name"].split("/")[0]
        source = syn_config["dir_name"].split("/")[1]
        source_table_name = syn_config.get("table_name")

        out_schema = syn_config['nfer_schema'] + ["VERSION", "UPDATED_BY"]

        if float(version) == float(self.SOURCE_DATA_VERSION) or is_base_version:  # FULL MODE
            versioned_syn_data_df = syn_data_df.withColumn("VERSION", F.lit(float(version)))
            versioned_syn_data_df = versioned_syn_data_df.withColumn("UPDATED_BY", F.lit(0.0))
        else:
            versioned_syn_data_df = self.gen_versioned_updates(syn_config, syn_data_df, version)

        # WRITE DATA n CALC STATS
        if 'NFER_PID' in syn_data_df.columns:
            versioned_syn_data_df = self.assign_file_id_for_nfer_pid(versioned_syn_data_df, source)
            versioned_syn_data_df = self.generate_row_id(versioned_syn_data_df)

        versioned_syn_data_df.cache()

        out_df = versioned_syn_data_df.select(out_schema)
        if source_table_name:
            out_dir = self.release_versioned_delta_table(out_df, source_table_name, version=version)
            if tag is not None:
                file_path = os.path.join(out_dir, tag)
                self.write_lines(file_path)
        else:
            self.write_final_dir(out_df, out_schema, source)

        counter = self.calc_stats(syn_config, versioned_syn_data_df, version, extra_counters)
        self.log_spark_stats(source_dir, source, counter, version=version)

    def gen_versioned_updates(self, syn_config, new_syn_df: DataFrame, version, ignore_cols=None):
        ignore_cols = ignore_cols or []
        syn_dir_name = syn_config['dir_name']
        nfer_schema = syn_config['nfer_schema']
        nfer_schema = list(set(nfer_schema) - set(ignore_cols))
        syn_schema = nfer_schema + ["VERSION", "UPDATED_BY"]
        syn_schema = [f for f in syn_schema if f != "ROW_ID"]
        unique_indices = syn_config['unique_indices']
        row_hash_fields = sorted(list(set(nfer_schema).difference(set(["ROW_ID"] + unique_indices + ignore_cols))))
        print("ROW_HASH_FIELDS:", row_hash_fields)

        # READ CURRENT FULL SYN DATA
        #new_syn_df = new_syn_df.withColumn("KEY_HASH", F.sha2(F.concat_ws("|", F.array(sorted(unique_indices))), 256))
        new_syn_df = self.calc_key_hash(new_syn_df,unique_indices=sorted(unique_indices),field="KEY_HASH")
        #new_syn_df = new_syn_df.withColumn("NEW_ROW_HASH", F.sha2(F.concat_ws("|", F.array(row_hash_fields)), 256))
        new_syn_df = self.calc_key_hash(new_syn_df, unique_indices=row_hash_fields, field="NEW_ROW_HASH")
        new_syn_df = new_syn_df.withColumn("VERSION", F.lit(float(version)))
        new_syn_df = new_syn_df.withColumn("UPDATED_BY", F.lit(0.0))

        # READ VERSIONED FULL SYN DATA
        table_name = syn_config.get('table_name', None)
        old_version = None
        try:
            if table_name:
                print(f"Table name present: {table_name}")
                #old_syn_dir = self.last_data_dir("DELTA_TABLES", "SYN_TABLES", table_name.upper() + ".parquet",
                                                 #run_version=version)
                old_syn_dir = self.latest_delta_dir("DELTA_TABLES", "SYN_TABLES", table_name.upper() + ".parquet")
                _, old_version = self.scan_data_dir("DELTA_TABLES", "SYN_TABLES", table_name.upper() + ".parquet", run_version=round(float(version) - 0.001, 3))
                print(f"Old syn dir: {old_syn_dir}")
                old_syn_df = self.read_delta_to_df(old_syn_dir)
            else:
                print(f"Table name not present")
                old_syn_dir = self.last_data_dir(syn_dir_name, run_version=version)
                print(f"Old syn dir: {old_syn_dir}")
                old_syn_df = self.read_final_dir(old_syn_dir)
                _, old_version = self.scan_data_dir(syn_dir_name, run_version=round(float(version) - 0.001, 3))
        except:
            print("Reading from Delta files")
            #old_syn_dir = self.last_data_dir("DELTA_TABLES","SYN_TABLES",syn_dir_name.split("/")[1]+".parquet", run_version=version)
            old_syn_dir = self.latest_delta_dir("DELTA_TABLES", "SYN_TABLES", syn_dir_name.split("/")[1] + ".parquet")
            old_syn_df = self.read_delta_to_df(old_syn_dir)
            #_, old_version = self.scan_data_dir("DELTA_TABLES", "SYN_TABLES", syn_dir_name.split("/")[1] + ".parquet",
                                                #run_version=round(float(version) - 0.001, 3))
            old_version = self.last_data_version("DELTA_TABLES", "SYN_TABLES", syn_dir_name.split("/")[1] + ".parquet")

        is_dim_syn_table = False
        if 'DIM_SYN' in syn_dir_name:
            is_dim_syn_table = True
        """
        # handles syn dim files
        is_dim_syn_table = False
        if 'DIM_SYN' not in syn_dir_name:
        

        else:
            schema = self.build_schema(nfer_schema,syn_config['type_schema'])
            old_syn_df = self.read_csv_to_df(old_syn_dir,sep='|',schema=schema)
            is_dim_syn_table = True
        """
        """
        if table_name == "DIM_SYN_DIAGNOSIS":
            old_syn_df = old_syn_df.withColumnRenamed("DIM_DIAGNOSIS_DK", "DIAGNOSIS_CODE_DK")
        old_syn_df = old_syn_df.select(nfer_schema)
        if "UPDATED_BY" not in old_syn_df.columns:
            old_syn_df = old_syn_df.withColumn("UPDATED_BY", F.lit(0.0))
        if "VERSION" not in old_syn_df.columns:
            old_syn_df = old_syn_df.withColumn("VERSION", F.lit(float(old_version)))
        """

        # old_syn_df = old_syn_df.withColumn("KEY_HASH", F.sha2(F.concat_ws("|", F.array(sorted(unique_indices))), 256))
        old_syn_df = self.calc_key_hash(old_syn_df, unique_indices=sorted(unique_indices), field="KEY_HASH")
        # old_syn_df = old_syn_df.withColumn("OLD_ROW_HASH", F.sha2(F.concat_ws("|", F.array(row_hash_fields)), 256))
        old_syn_df = self.calc_key_hash(old_syn_df, unique_indices=row_hash_fields, field="OLD_ROW_HASH")
        old_syn_df.drop("ROW_ID").cache()
        print("OLD SYN DATA COUNT = ", old_syn_df.count())

        # IDENTIFY ROW UPDATES & DELETES IN OLD SYN DATA
        if is_dim_syn_table:
            new_hash_df = new_syn_df.select("KEY_HASH", "NEW_ROW_HASH", "UPDATED_BY")
            final_data_df = old_syn_df.join(new_hash_df, ["KEY_HASH", "UPDATED_BY"], how='LEFT')
        else:
            new_hash_df = new_syn_df.select("NFER_PID", "KEY_HASH", "NEW_ROW_HASH", "UPDATED_BY")
            final_data_df = old_syn_df.join(new_hash_df, ["NFER_PID", "KEY_HASH", "UPDATED_BY"], how='LEFT')

        final_data_df = final_data_df.na.fill("", subset=["NEW_ROW_HASH"])
        final_data_df = final_data_df.withColumn("UPDATED_BY",
                                                 F.when(F.col("UPDATED_BY").isin(0.0) & (
                                                F.col("OLD_ROW_HASH") != F.col("NEW_ROW_HASH")),
                                                F.lit(float(version)))
                                                 .otherwise(F.col("UPDATED_BY")))

        # DROP ROWS WITHOUT UPDATES IN NEW SYN DATA
        if is_dim_syn_table:
            old_hash_df = old_syn_df.select("KEY_HASH", "OLD_ROW_HASH", "UPDATED_BY")
            new_syn_df = new_syn_df.join(old_hash_df, ["KEY_HASH", "UPDATED_BY"], how='LEFT')
        else:
            old_hash_df = old_syn_df.select("NFER_PID", "KEY_HASH", "OLD_ROW_HASH", "UPDATED_BY")
            new_syn_df = new_syn_df.join(old_hash_df, ["NFER_PID", "KEY_HASH", "UPDATED_BY"], how='LEFT')

        new_syn_df = new_syn_df.na.fill("", ["OLD_ROW_HASH"])
        new_syn_df = new_syn_df.filter(F.col("OLD_ROW_HASH") != F.col("NEW_ROW_HASH"))

        # CONCATENATE UPDATES
        new_syn_df = new_syn_df.select(syn_schema)
        final_data_df = final_data_df.select(syn_schema)
        final_data_df = final_data_df.unionByName(new_syn_df).drop("ROW_ID")
        return final_data_df

    def gen_spark_sql_delta_tables(self):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        print("Generating SYN_DATA Delta Tables for ", version)
        for syn_source, syn_config in self.SYSTEM_DICT["syn_tables"].items():
            start = time.perf_counter()
            if self.options.source and syn_source not in self.options.source:
                continue

            syn_dir_name = syn_config.get('dir_name')
            if not syn_dir_name:
                print("Skipping ", syn_dir_name)
                continue

            syn_data_dir = os.path.join(self.ROOT_DIR, version, syn_config["dir_name"])
            if not self.glob(syn_data_dir):
                print("Data not available, Skipping ", syn_data_dir)
                continue
            syn_table_name = "FACT_SYN_" + syn_config['dir_name'].split('/')[-1]
            print(f'SYN_TABLE: {syn_table_name}')
            spark: SparkSession = self.init_spark_session()
            syn_data_df = self.read_final_dir(syn_data_dir).drop("FILE_ID")
            if "VERSION" not in syn_data_df.columns:
                syn_data_df = syn_data_df.withColumn("VERSION", F.lit(float(self.SOURCE_DATA_VERSION)))
                syn_data_df = syn_data_df.withColumn("UPDATED_BY", F.lit(0.0))

            self.release_versioned_delta_table(syn_data_df, syn_table_name, write_latest_delta=True)
            end = time.perf_counter()
            print(syn_source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("SPARK_SQL: Total Time =", end - begin)

    def parquet_to_final(self):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        print("Generating SYN_DATA Delta Tables for ", version)
        for syn_source, syn_config in self.SYSTEM_DICT["syn_tables"].items():
            start = time.perf_counter()
            if self.options.source and syn_source not in self.options.source:
                continue

            syn_dir_name = syn_config.get('dir_name')
            if not syn_dir_name:
                print("Skipping ", syn_dir_name)
                continue
            out_dir = os.path.join(self.LOCAL_ROOT_DIR, version, syn_dir_name)

            syn_table_name = syn_config.get('table_name', f"FACT_SYN_{syn_dir_name.split('/')[-1]}")
            if not self.glob(out_dir) and self.skip_if_exists:
                print(f"Already generated.. skipping {syn_table_name}")
            syn_table_path = os.path.join(self.ROOT_DIR, version, "DELTA_TABLES", "SYN_TABLES",
                                          f"{syn_table_name}.parquet")
            if not self.glob(syn_table_path):
                print("Data not available, Skipping ", syn_table_path)
                continue

            print(f'SYN_TABLE: {syn_table_name}')
            self.init_spark_session()
            syn_data_df = self.read_delta_to_df(syn_table_path)

            if 'NFER_PID' not in syn_data_df.columns:
                self.write_df_to_csv(syn_data_df, os.path.join(out_dir, 'output.csv'))
            else:
                self.df_to_patient_blocks(syn_data_df, out_dir, syn_data_df.columns)
            end = time.perf_counter()

            print(syn_source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("PARQUET_TO_FINAL: Total Time =", end - begin)

    def validate_syn_data(self, syn_config, syn_data_df: DataFrame):
        unique_indices = syn_config['unique_indices']
        if "NFER_PID" in syn_data_df.columns:
            window = Window.partitionBy(unique_indices).orderBy(F.col("NFER_PID"))
        else:
            window = Window.partitionBy(unique_indices).orderBy(
                sorted([c for c in syn_data_df.columns if c not in unique_indices]))
        validate_syn_df = syn_data_df.withColumn("ROW_NUM", F.row_number().over(window))
        validate_syn_df = validate_syn_df.withColumn("ROW_COUNT", F.lit(1))
        validate_syn_df = validate_syn_df.withColumn("DUPE_COUNT",
                                                     F.when(F.col("ROW_NUM") > 1, F.lit(1)).otherwise(F.lit(0)))
        validate_syn_df = validate_syn_df.groupBy().agg(
            F.sum("ROW_COUNT").alias("ROW_COUNT"),
            F.sum("DUPE_COUNT").alias("DUPE_COUNT")
        ).collect()

        syn_data_count = validate_syn_df[0]['ROW_COUNT'] if validate_syn_df[0]['ROW_COUNT'] else 0
        print("SYN DATA COUNT =", syn_data_count)
        dupe_count = validate_syn_df[0]['DUPE_COUNT'] if validate_syn_df[0]['DUPE_COUNT'] else 0
        print("DUPLICATE SYN DATA COUNT = ", dupe_count)
        if dupe_count > 0:
            raise Exception(f"{dupe_count} DUPLICATES FOUND IN {syn_config['table_name']}")
        return syn_data_count

    def calc_stats(self, syn_config, syn_data_df: DataFrame, version: str, extra_counters: dict):
        counter = Counter()
        """
        # disabling legacy code
        extra_counters['rows_total'] = F.col("UPDATED_BY") >= F.lit(0.0)
        extra_counters['rows_active'] = F.col("UPDATED_BY").isin(0.0)
        extra_counters['rows_inserted'] = F.col("VERSION").isin(float(version))
        extra_counters['rows_updated'] = F.col("UPDATED_BY").isin(float(version))
        for count, myfilter in extra_counters.items():
            counter.update({count: syn_data_df.filter(myfilter).count()})
        """

        counter.update({'rows_total': syn_data_df.filter(F.col("UPDATED_BY") >= F.lit(0.0)).count()})
        counter.update({'rows_active': syn_data_df.filter(F.col("UPDATED_BY").isin(0.0)).count()})
        counter.update({'rows_inserted': syn_data_df.filter(F.col("VERSION").isin(float(version))).count()})
        counter.update({'rows_updated': syn_data_df.filter(F.col("UPDATED_BY").isin(float(version))).count()})

        if 'NFER_PID' in syn_data_df.columns:
            counter.update({'patients_active': syn_data_df.filter(F.col("UPDATED_BY").isin(0.0)).select(
                "NFER_PID").distinct().count()})

            if float(version) != float(self.SOURCE_DATA_VERSION):  # DELTA MODE
                new_syn_df = syn_data_df.filter(F.col("UPDATED_BY").isin(0.0))
                new_syn_df = new_syn_df.groupBy("NFER_PID").agg(F.count("*").alias("NEW_COUNT"))
                last_version_dir = self.last_data_dir(syn_config['dir_name'], run_version=version)
                if last_version_dir:
                    last_version = float(last_version_dir.split("/")[-3])
                    last_syn_df = self.past_version_filter(syn_data_df, last_version)
                    last_syn_df = last_syn_df.groupBy("NFER_PID").agg(F.count("*").alias("OLD_COUNT"))
                    diff_df = last_syn_df.join(new_syn_df, ["NFER_PID"], "LEFT")
                    counter.update({'patients_dropped': diff_df.filter(F.col("NEW_COUNT").isNull()).count()})

        return counter

    def check_for_source_updates(self, source_list):
        source_update_found = not self.skip_if_exists
        for source in source_list:
            if self.data_dir("DATAGEN", source):
                source_update_found = True
        return source_update_found

    def format_harmonized_variables(self, source_type):
        # READ DATA
        start = time.perf_counter()
        spark: SparkSession = self.init_spark_session()
        syn_config = self.SYSTEM_DICT["syn_tables"][source_type]
        nfer_schema = syn_config['nfer_schema']
        source_dir = syn_config["source_dir"]

        # VAR UNITS
        variable_meta_path = os.path.join(source_dir, "variable.tsv")
        variable_meta_df = self.read_csv_to_df(variable_meta_path, sep='$$$')
        variable_meta_df = variable_meta_df.select("VariableID", "Units", "DisplayName")
        variable_meta_df = variable_meta_df.withColumnRenamed("Units", "HARMONIZED_UNITS")

        # This is reading the full data and filtering out the lab test variables
        # This is slow but will be fixed when data in flow_sheets and lab_tests are in seperate folders
        folder_paths = glob.glob(os.path.join(source_dir, "files_*"))
        source_df = None

        # TODO Remove when data is in separate folders
        if source_type == "nfer_harmonized_vars":
            read_columns = ["VARIABLE_NAME",
                            "VariableID",
                            "NFER_PID",
                            "ROW_ID",
                            "NFER_DTM",
                            "DERIVED_TYPE",
                            "DERIVED_VALUE",
                            "LAB_TEST_DK",
                            "NORMAL_RANGE_TXT",
                            "RESULT_TXT",
                            "UNIT_OF_MEASURE_TXT"]
        elif source_type == "harmonized_flowsheets":
            read_columns = ["VARIABLE_NAME",
                            "VariableID",
                            "NFER_PID",
                            "ROW_ID",
                            "NFER_DTM",
                            "FLOWSHEET_ROW_DESCRIPTION",
                            "FLOWSHEET_RESULT_TXT",
                            "FLOWSHEET_ROW_NAME_DK",
                            "DERIVED_TYPE",
                            "DERIVED_VALUE",
                            "FLOWSHEET_UNIT_OF_MEASURE_TXT",
                            "FLOWSHEET_SUBTYPE_DESCRIPTION",
                            "FLOWSHEET_ROW_CAPTURED_DTM",
                            "FLOWSHEET_TYPE_DESCRIPTION"
                            ]
        for folder in folder_paths:
            source_path = os.path.join(folder, 'patient_*.final')
            header_file = os.path.join(folder, 'header.csv')
            header_files = glob.glob(header_file)
            source_files = glob.glob(source_path)
            if len(header_files) > 0 and len(source_files) > 0:
                header_file = header_files[0]
                with open(header_file, 'r') as f:
                    header = f.readline().strip().split("|")

                    # TODO Remove the checks when data is in separate folders
                    if source_type == "nfer_harmonized_vars":
                        if "LAB_TEST_DK" not in header:
                            continue
                    elif source_type == "harmonized_flowsheets":
                        if "FLOWSHEET_ROW_NAME_DK" not in header:
                            continue
                partial_df = self.read_final_dir(source_files, header_file=header_files[0]).drop("FILE_ID")
                partial_df = partial_df.withColumnRenamed("variable_name", "VARIABLE_NAME")
                partial_df = partial_df.select(read_columns)
                if source_df is None:
                    source_df = partial_df
                else:
                    source_df = source_df.union(partial_df)

        source_df = source_df.join(F.broadcast(variable_meta_df), ["VariableID"])

        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT",
                                                         ["FILE_ID", "NFER_PID", "BIRTH_DATE", "PATIENT_MERGED_FLAG"])
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)
        source_df = source_df.join(F.broadcast(dim_patient_df), ["NFER_PID"])

        # NFER_AGE
        source_df = self.calc_nfer_age_for_nfer_dtm(source_df)
        source_df = self.assign_file_id_for_nfer_pid(source_df, "FACT_LAB_TEST")
        source_df = source_df.withColumnRenamed("VariableID", "VARIABLE_ID")
        source_df = source_df.withColumn("VERSION", F.lit(4.000))
        source_df = source_df.withColumn("UPDATED_BY", F.lit(0.0))

        # WRITE DATA
        out_dir = os.path.join(self.DATA_DIR, syn_config["dir_name"])
        self.df_to_patient_blocks(source_df, out_dir, nfer_schema)

        end = time.perf_counter()
        print("HARMONIZED_LAB_TEST : Total Time =", end - start)

    def generate_infox_data(self):
        # READ DATA
        start = time.perf_counter()
        self.init_spark_session(expand_partitions=True)
        syn_config = self.SYSTEM_DICT["syn_tables"]["infox_data"]
        nfer_schema = syn_config['nfer_schema']
        source_dir = syn_config["source_dir"]
        # out_dir_name = syn_config["dir_name"]
        source_paths = self.glob(os.path.join(source_dir, "FACT_*.txt"))
        header_file = self.glob(os.path.join(source_dir, "header.csv"))[0]
        source_df = self.read_final_dir(source_paths, header_file=header_file)

        # CREATE DIM TABLES FOR REQUIRED ENTITIES
        dims = ['ENTITY_1_PREF_NAME', 'ENTITY_2_PREF_NAME']
        source_df_dims = source_df.select(*dims).distinct()
        source_df_dims.cache()

        # CREATE ALL DIM TABLES
        for dim in dims:
            source_df_entity = source_df_dims.select(dim).distinct()
            windowSpec = Window.orderBy(dim)
            source_df_entity = source_df_entity.withColumn("ID", F.row_number().over(windowSpec))
            source_df_entity = source_df_entity.withColumnRenamed(dim, "ENTITY_PREF_NAME")

            # WRITE DATA
            out_dir = os.path.join(self.DATA_DIR, "SYN_TABLES", f"DIM_SYN_{dim}")
            self.write_df_to_delta_lake(source_df_entity, out_dir)

            # JOIN WITH FULL DF
            source_df = source_df.join(source_df_entity, source_df[dim] == source_df_entity["ENTITY_PREF_NAME"], "left")
            source_df = source_df.withColumnRenamed("ID", f"{dim}_ID")
            source_df = source_df.drop("ENTITY_PREF_NAME")

        # PROCESS INFOX EXTRACTIONS
        # source_dir = "gs://cdap-offline-data/InfoX/data_dump_v4/revised_dump/"

        source_df = source_df.withColumn("MODEL_TYPE", F.when(F.col("PREDICTION_TYPE").isin(
            "Has_Disease", "May_Have_Disease", "Does_Not_Have_Disease", "Received_Medication",
            "Family_History_For_Disease", "No_Family_History_For_Disease", "Procedure_Performed"),
            F.lit("Sentiments")).when(F.col("PREDICTION_TYPE").isin(
            "Adverse_Event", "Disease_Stage", "Negative_For_Biomarker",
            "Positive_For_Biomarker", "Potential_Adverse_Event", "Therapeutic",
            "Associated_Disease_Location", "Associated_Diagnosis_Date",
            "Gene_Associated_With_Mutation"), F.lit("Associations")).otherwise(F.lit("")))
        source_df = source_df.withColumn("NFER_DTM", F.when(((F.col(
            "DATE_PUBLISHED") <= -2209008070) | (F.col("DATE_PUBLISHED") > 1988150399)), F.lit(None)).otherwise(
            F.col("DATE_PUBLISHED")))

        source_df = source_df.withColumnRenamed("COLLECTION_NAME", "SOURCE_TABLE")

        source_df = source_df.withColumn("META_INFORMATION", F.from_json(
            F.col("META_INFORMATION"), MapType(StringType(), StringType())))

        windowSpec = Window.partitionBy("NFER_PID", "ENTITY_1_PREF_NAME", "ENTITY_2_PREF_NAME",
                                        "SENTENCE_INDEX").orderBy("NFER_DTM")
        source_df = source_df.withColumn("ROW_NUMBER", F.row_number().over(windowSpec))
        source_df = source_df.withColumn("IS_DUPLICATE", F.when(F.col("ROW_NUMBER") == 1, False).otherwise(True))
        source_df = source_df.withColumn("VERSION", F.lit(4.000))
        source_df = source_df.withColumn("UPDATED_BY", F.lit(0.0))

        # WRITE DATA
        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "FACT_AUGMENTED_CURATION")
        self.write_df_to_delta_lake(source_df, out_dir, nfer_schema)

        # PROCESS UNIQUE SENTENCES
        print("Processing unique sentences")
        source_paths = self.glob(os.path.join(source_dir, "FACT_*", "unique_sentences_FACT_*.txt"))
        header_file = self.glob(os.path.join(source_dir, "FACT_C*", "header_unique_sentences.csv"))[0]
        source_df = self.read_final_dir(source_paths, header_file=header_file)

        source_df = source_df.withColumn("SOURCE_TABLE", F.split(F.input_file_name(), '/')[5])
        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "DIM_AUGMENTED_CURATIONS_UNIQUE_SENTENCES")
        schema = ["SENTENCE_INDEX", "SENTENCE", "SOURCE_TABLE", "SENTENCE_COUNT"]
        self.write_df_to_delta_lake(source_df, out_dir, schema)

        # PROCESS NOTE_ID SENETENCE_ID MAP
        print("Processing note_id sentence_id map")
        source_paths = self.glob(os.path.join(source_dir, "FACT_*", "note_id_FACT_*.txt"))
        header_file = self.glob(os.path.join(source_dir, "FACT_C*", "header_note_id.csv"))[0]
        source_df = self.read_final_dir(source_paths, header_file=header_file, sep=TAB_SEP)

        source_df = source_df.withColumn("SOURCE_TABLE", F.split(F.input_file_name(), '/')[5])
        source_df = source_df.withColumn("SENTENCE_INDEXES", F.split(F.col("SENTENCE_INDEX"), '\\|'))
        source_df = source_df.withColumn("SENTENCE_INDEXES", source_df.SENTENCE_INDEXES.cast("array<long>"))
        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "DIM_AUGMENTED_CURATIONS_NOTE_SENTENCE_MAP")
        schema = ["NOTE_ID", "SENTENCE_INDEXES", "SOURCE_TABLE"]
        self.write_df_to_delta_lake(source_df, out_dir, schema)

        end = time.perf_counter()
        print("INFOX_DATA : Total Time =", end - start)

    def generate_infox_disease_association_dfs_for_hyc(self):
        start = time.perf_counter()
        self.init_spark_session(expand_partitions=True)

        syn_config = self.SYSTEM_DICT["syn_tables"]["infox_data_hyc"]
        source_dir = syn_config["source_dir"]
        # source_dir = "gs://cdap-offline-data/InfoX/data_dump_v4/revised_dump/"

        source_paths = self.glob(os.path.join(
            source_dir, "FACT_*.txt"))
        header_file = self.glob(os.path.join(source_dir, "header.csv"))[0]
        source_df = self.read_final_dir(source_paths, header_file=header_file)

        valid_prediction_types = ["Has_Disease", "Therapeutic", "Adverse_Event", "Potential_Adverse_Event",
                                  "Disease_Stage", "Negative_For_Biomarker", "Positive_For_Biomarker"]
        source_df = source_df.filter(F.col("PREDICTION_TYPE").isin(valid_prediction_types))
        source_df = source_df.filter(F.col("PROBABILITY_SCORES") > 0.8)
        source_df = source_df.withColumn("NFER_DTM", F.when(((F.col(
            "DATE_PUBLISHED") <= -2209008070) | (F.col("DATE_PUBLISHED") > 1988150399)), F.lit(None)).otherwise(
            F.col("DATE_PUBLISHED")))

        source_df = source_df.withColumn("INFOX_DISEASE",
                                         F.when(F.col("PREDICTION_TYPE").isin("Therapeutic", "Adverse_Event",
                                                                              "Potential_Adverse_Event"),
                                                F.col("ENTITY_2_PREF_NAME"))
                                         .otherwise(F.col("ENTITY_1_PREF_NAME")))
        source_df = source_df.withColumn("INFOX_DRUG", F.when(
            F.col("PREDICTION_TYPE").isin("Therapeutic", "Adverse_Event", "Potential_Adverse_Event"),
            F.col("ENTITY_1_PREF_NAME"))
                                         .otherwise(F.lit(None).cast(StringType())))
        source_df = source_df.withColumn("INFOX_DISEASE_STAGE", F.when(F.col("PREDICTION_TYPE").isin(
            "Disease_Stage"), F.col("ENTITY_2_PREF_NAME")).otherwise(F.lit(None).cast(StringType())))
        source_df = source_df.withColumn("INFOX_BIOMARKER", F.when(F.col("PREDICTION_TYPE").isin(
            "Negative_For_Biomarker", "Positive_For_Biomarker"), F.col("ENTITY_2_PREF_NAME")).otherwise(
            F.lit(None).cast(StringType())))
        source_df = source_df.withColumn(
            "NFER_DTM", source_df["NFER_DTM"] - source_df["NFER_DTM"] % (60 * 60 * 24))
        source_df = source_df.withColumn("NFER_DTM", F.col("NFER_DTM").cast(LongType()))

        # Logic to retain only those models that occur in 2 separate sentences
        # Take max of dense rank
        windowSpec = Window.partitionBy("NFER_PID", "PREDICTION_TYPE", "ENTITY_1_PREF_NAME",
                                        "ENTITY_2_PREF_NAME").orderBy("SENTENCE_HASH")
        source_df = source_df.withColumn("d_rnk", F.dense_rank().over(windowSpec))
        max_window_spec = Window.partitionBy("NFER_PID", "PREDICTION_TYPE", "ENTITY_1_PREF_NAME", "ENTITY_2_PREF_NAME")
        source_df = source_df.withColumn("max_predict_flag", F.max("d_rnk").over(max_window_spec))
        source_df = source_df.filter(F.col("max_predict_flag") > 1)

        source_df = source_df.select("NFER_PID", "NFER_DTM", "INFOX_DISEASE", "INFOX_DRUG", "INFOX_DISEASE_STAGE",
                                     "INFOX_BIOMARKER", "PREDICTION_TYPE", "SENTENCE_HASH")
        source_df = source_df.persist(StorageLevel.DISK_ONLY)

        drug_df = source_df.filter(
            F.col("PREDICTION_TYPE").isin("Therapeutic", "Adverse_Event", "Potential_Adverse_Event"))
        drug_df = drug_df.select("SENTENCE_HASH", "INFOX_DISEASE", "INFOX_DRUG", "PREDICTION_TYPE")
        drug_df = drug_df.withColumnRenamed("PREDICTION_TYPE", "INFOX_DRUG_ASSOCIATION")
        # drug_df = drug_df.groupBy("INFOX_DISEASE", "INFOX_DRUG_ASSOCIATION", "SENTENCE_HASH").agg(
        #    F.concat_ws(" nferdelimnfer ", F.collect_set("INFOX_DRUG")).alias("INFOX_DRUG"))

        stage_df = source_df.filter(F.col("PREDICTION_TYPE").isin("Disease_Stage"))
        stage_df = stage_df.select("SENTENCE_HASH", "INFOX_DISEASE", "INFOX_DISEASE_STAGE")
        # stage_df = stage_df.groupBy("INFOX_DISEASE", "SENTENCE_HASH").agg(F.concat_ws(
        #    " nferdelimnfer ", F.collect_set("INFOX_DISEASE_STAGE")).alias("INFOX_DISEASE_STAGE"))

        biomarker_df = source_df.filter(
            F.col("PREDICTION_TYPE").isin("Negative_For_Biomarker", "Positive_For_Biomarker"))
        biomarker_df = biomarker_df.select("SENTENCE_HASH", "INFOX_DISEASE", "INFOX_BIOMARKER", "PREDICTION_TYPE")
        biomarker_df = biomarker_df.withColumnRenamed("PREDICTION_TYPE", "INFOX_BIOMARKER_ASSOCIATION")
        biomarker_df = biomarker_df.withColumn("INFOX_BIOMARKER_ASSOCIATION",
                                               F.when(F.col("INFOX_BIOMARKER_ASSOCIATION") == "Positive_For_Biomarker",
                                                      "Positive").otherwise("Negative"))
        # biomarker_df = biomarker_df.groupBy("INFOX_DISEASE", "INFOX_BIOMARKER_ASSOCIATION", "SENTENCE_HASH").agg(
        #    F.concat_ws(" nferdelimnfer ", F.collect_set("INFOX_BIOMARKER")).alias("INFOX_BIOMARKER"))

        association_df = drug_df.join(stage_df, ["SENTENCE_HASH", "INFOX_DISEASE"], "outer")
        association_df = association_df.join(biomarker_df, ["SENTENCE_HASH", "INFOX_DISEASE"], "outer")
        association_schema = ["INFOX_DISEASE", "INFOX_DRUG_ASSOCIATION", "INFOX_DRUG", "INFOX_DISEASE_STAGE",
                              "INFOX_BIOMARKER_ASSOCIATION", "INFOX_BIOMARKER", "SENTENCE_HASH"]
        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "ASSOCIATION_DF")
        self.write_df(association_df, out_dir, association_schema)

        disease_df = source_df.filter(F.col("PREDICTION_TYPE").isin("Has_Disease"))
        disease_df = disease_df.withColumnRenamed("PREDICTION_TYPE", "INFOX_DISEASE_SENTIMENT")
        disease_df = disease_df.select("NFER_PID", "NFER_DTM", "SENTENCE_HASH", "INFOX_DISEASE",
                                       "INFOX_DISEASE_SENTIMENT").distinct()
        disease_schema = ["NFER_PID", "NFER_DTM", "SENTENCE_HASH", "INFOX_DISEASE", "INFOX_DISEASE_SENTIMENT"]
        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "DISEASE_DF")
        self.write_df(disease_df, out_dir, disease_schema)

        end = time.perf_counter()
        print("INFOX_DATA : Total Time =", end - start)

    def generate_infox_final_df_for_hyc(self):
        start = time.perf_counter()
        self.init_spark_session(expand_partitions=True)

        disease_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "DISEASE_DF")
        source_paths = glob.glob(os.path.join(disease_dir, "part*.csv"))
        header_file = os.path.join(disease_dir, "header.csv")
        disease_df = self.read_final_dir(source_paths, header_file)

        association_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "ASSOCIATION_DF")
        source_paths = glob.glob(os.path.join(association_dir, "part*.csv"))
        header_file = os.path.join(association_dir, "header.csv")
        association_df = self.read_final_dir(source_paths, header_file)
        if "UNIQUE_HASH" in association_df.columns:
            association_df = association_df.drop("UNIQUE_HASH")

        final_df = disease_df.join(association_df, ["SENTENCE_HASH", "INFOX_DISEASE"], "left")

        # DROP Duplicate sentences for a patient
        # window = Window.partitionBy("NFER_PID","SENTENCE_HASH").orderBy("NFER_DTM")
        # final_df = final_df.withColumn("ROW_ID", F.row_number().over(window))
        # final_df = final_df.filter(F.col("ROW_ID") == 1)

        final_df = final_df.select("NFER_PID", "NFER_DTM", "INFOX_DISEASE", "INFOX_DISEASE_SENTIMENT",
                                   "INFOX_DRUG_ASSOCIATION", "INFOX_DRUG", "INFOX_DISEASE_STAGE",
                                   "INFOX_BIOMARKER_ASSOCIATION", "INFOX_BIOMARKER").distinct()

        hyc_df = self.read_hyc_corpus()
        hyc_df = hyc_df.withColumnRenamed("NFER_DTM", "NFER_DTM_FINAL")
        hyc_df = hyc_df.withColumn(
            "NFER_DTM", hyc_df["NFER_DTM_FINAL"] - hyc_df["NFER_DTM_FINAL"] % (60 * 60 * 24))
        hyc_df = hyc_df.groupBy("NFER_PID", "NFER_DTM").agg(F.first("NFER_DTM_FINAL").alias("NFER_DTM_FINAL"))
        final_df = final_df.join(hyc_df, ["NFER_PID", "NFER_DTM"], "left")
        final_df = final_df.withColumn("NFER_DTM",
                                       F.when(F.col("NFER_DTM_FINAL").isNull(), F.col("NFER_DTM")).otherwise(
                                           F.col("NFER_DTM_FINAL")))
        final_df = final_df.drop("NFER_DTM_FINAL")

        final_df_schema = [
            "NFER_PID",
            "NFER_DTM",
            "INFOX_DISEASE",
            "INFOX_DISEASE_SENTIMENT",
            "INFOX_DRUG",
            "INFOX_DRUG_ASSOCIATION",
            "INFOX_DISEASE_STAGE",
            "INFOX_BIOMARKER",
            "INFOX_BIOMARKER_ASSOCIATION"
        ]

        out_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "FINAL_DF_DISTINCT")
        self.write_df(final_df, out_dir, final_df_schema)
        end = time.perf_counter()
        print("INFOX_DATA Generate FINAL_DF : Total Time =", end - start)

    def generate_infox_patient_blocks_for_hyc(self):
        start = time.perf_counter()
        self.init_spark_session(expand_partitions=True)
        syn_config = self.SYSTEM_DICT["syn_tables"]["infox_data_hyc"]
        out_dir = syn_config["dir_name"]
        nfer_schema = syn_config["nfer_schema"]

        final_df_distinct_dir = os.path.join(self.DATA_DIR, "SYN_DATA", "INFOX_INTERIM", "FINAL_DF_DISTINCT")
        source_paths = glob.glob(os.path.join(final_df_distinct_dir, "part*.csv"))
        header_file = os.path.join(final_df_distinct_dir, "header.csv")
        final_df = self.read_final_dir(source_paths, header_file).drop("FILE_ID")

        dim_patient_df = self.read_versioned_datagen_dir("DIM_PATIENT",
                                                         ["NFER_PID", "BIRTH_DATE", "PATIENT_MERGED_FLAG"])
        dim_patient_df = self.de_dupe_patient_meta(dim_patient_df)

        final_df = final_df.join(F.broadcast(dim_patient_df), ["NFER_PID"])
        final_df = self.calc_nfer_age_for_nfer_dtm(final_df)
        final_df = self.assign_file_id_for_nfer_pid(final_df, "DIM_PATIENT")
        final_df = final_df.withColumn("VERSION", F.lit(4.000))
        final_df = final_df.withColumn("UPDATED_BY", F.lit(0.0))
        final_df = final_df.cache()

        out_dir = os.path.join(self.DATA_DIR, out_dir)
        self.df_to_patient_blocks(final_df, out_dir, nfer_schema)

        end = time.perf_counter()
        print("INFOX_DATA Generate patient blocks : Total Time =", end - start)

    def read_hyc_corpus(self) -> DataFrame:
        # pts_final.corpus used to match NFER_DTM(folded to day)
        # to closer existing DTMS in HyC
        path = "/datagen_disk/HYC_FINAL/pts_final.corpus"
        schema = StructType([
            StructField("NFER_PID", IntegerType(), True),
            StructField("NFER_DTM", LongType(), True),
        ])
        hyc_df = self.read_csv_to_df(path, schema, header=False)
        return hyc_df


if __name__ == '__main__':
    SynDataGenJob().run()
