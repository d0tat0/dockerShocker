import os
import time
from collections import Counter

from pyspark.sql import *
from pyspark.sql import functions as F

from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator


class FactGenJob(Orchestrator):

    def __init__(self):
        super().__init__()
        self.parent = self

    def run(self):
        table_list = self.parse_options_source()
        if 'fact_gen' in self.options.run:
            self.generate_fact_data(table_list)
        if 'delta_table' in self.options.run:
            self.gen_spark_sql_delta_tables(table_list)
        if 'patient_map_json' in self.options.run:
            self.generate_patient_map_json()
        if 'genomic' in self.options.run:
            self.generate_genomic_data()
        if 'parquet2final' in self.options.run:
            self.parquet_to_final(table_list)
        if 'parquet2finalbucket' in self.options.run:
            self.parquet_to_final_bucket(table_list)
        if 'upload_harmonized_misc' in self.options.run:
            self.upload_harmonized_interim_misc_data(table_list)

    def get_dim_patients_df(self,version=None):
        schema = ["NFER_PID", self.BIRTH_DATE]
        if self.RELATIVE_DTM:
            schema.append(self.ENCOUNTER_EPOCH)
        if self.PATIENT_MERGED_FLAG:
            schema.append("PATIENT_MERGED_FLAG")

        patient_maps_df = self.read_versioned_datagen_dir(self.DIM_PATIENT, columns=schema,version=version)
        return self.de_dupe_patient_meta(patient_maps_df)

    def generate_fact_data(self, source_list):
        """
        read preprocessed files and generate required fields
        """
        begin = time.perf_counter()
        self.init_spark_session()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        patient_map_df = self.read_versioned_dim_maps(columns=[self.PATIENT_DK, "NFER_PID"],version=run_version)
        dim_patient_df = self.get_dim_patients_df(version=run_version)
        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            if self.skip_if_exists and self.data_dir("DATAGEN", source,run_version=run_version):
                print(f'{source} already exists. Skipping generation.')
                continue

            _, nfer_schema, _ = self.read_schema(source)
            timestamp_fields = self.get_timestamp_fields(source)
            self.init_spark_session(source)
            source_df = self.read_source_files(source, version=run_version)
            if source_df is None:
                print(f'No data found for {source}')
                continue
            source_df = self.assign_nfer_pid(source_df, patient_map_df)
            source_df = self.assign_file_id_for_nfer_pid(source_df, source)
            source_df = self.parse_dtm_and_calc_nfer_age(source, source_df, dim_patient_df, timestamp_fields, run_version)
            source_df = self.dim_dk_hash_to_int(source, source_df)
            source_df = self.addon_fact_hash_fields(source, source_df)
            source_df, extraction_fields, extraction_tag = self.addon_extraction_fields(source_df, source)
            source_df = self.increment_row_id(source, source_df,version=run_version)
            final_schema = nfer_schema + extraction_fields
            if source not in ['FACT_FLOWSHEETS'] or self.RUN_MODE == RunMode.Delta.value:
                source_df.cache()

            order_by = None
            if source == "FACT_ECG_WAVEFORM":
                order_by = ["NFER_PID", "NFER_DTM", "WAVEFORM_TYPE", "LEAD_ID"]

            self.write_final_dir("DATAGEN", source_df, final_schema, source, order_by=order_by,version=run_version)
            counter = self.calc_stats(source_df, extraction_fields)
            self.validate_stats(source, counter)
            self.validate_fact_counts(source, counter,version=run_version)

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)
            counter.update({"run_time": end - start})
            counter = dict(counter)
            if extraction_tag:
                counter.update({"extraction_tag": extraction_tag})
            self.log_spark_stats("DATAGEN", source, counter,version=run_version)
            self.SPARK_SESSION.catalog.clearCache()

        end = time.perf_counter()
        print("FACT GEN: Total Time =", end - begin)

    def assign_nfer_pid(self, source_df: DataFrame, patient_map_df: DataFrame) -> DataFrame:
        source_df = source_df.join(F.broadcast(patient_map_df), [self.PATIENT_DK], "LEFT")
        current_cols = source_df.columns
        for _field in self.ADDITIONAL_PDK:
            if _field not in current_cols:
                continue
            new_field = _field.replace(self.PATIENT_DK, "NFER_PID")
            patient_map_df = patient_map_df.withColumnRenamed("NFER_PID", new_field).withColumnRenamed(self.PATIENT_DK,
                                                                                                       _field)
            source_df = source_df.join(F.broadcast(patient_map_df), _field, "LEFT")
        source_df = source_df.na.fill(0, ["NFER_PID"])
        return source_df

    def get_encounter_joined_dtm(self, source_df, join_col, version=None):
        edf = self.read_source_files("ENCOUNTER", version=version).select(["PAT_ENC_CSN_ID", "ENCOUNTER_START_DATE"])
        edf = self.parse_timestamp(edf, "ENCOUNTER_START_DATE")
        drop_cols = ["PAT_ENC_CSN_ID", "ENCOUNTER_START_DATE"]
        if join_col == "PAT_ENC_CSN_ID":
            drop_cols = ["ENCOUNTER_START_DATE"]
            source_df = source_df.join(edf, join_col, how="left")
        else:
            source_df = source_df.join(edf, source_df[join_col] == edf["PAT_ENC_CSN_ID"], how="left")
        source_df = source_df.withColumn("NFER_DTM", F.coalesce(source_df["ENCOUNTER_START_DATE"]))
        source_df = source_df.drop(*drop_cols)
        return source_df

    def parse_dtm_and_calc_nfer_age(self, source, source_df: DataFrame, dim_patient_df: DataFrame, timestamp_fields, version=None) -> DataFrame:
        print('DTM FIELDS:', timestamp_fields)
        source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
        dtm_name = source_config.get("dtm")
        if not dtm_name:
            return source_df

        dtm_format = source_config.get("dtm_format", None)
        use_default = False if dtm_format else source_config.get("use_default_dtm_regex", True)
        source_df = source_df.join(F.broadcast(dim_patient_df), ["NFER_PID"], "LEFT")
        for field in timestamp_fields:
            source_df = self.parse_timestamp(source_df, field, custom_dtm_format=dtm_format, use_default=use_default)

        """Until Duke fixes the DTM field, joining with encounter table to get the correct DTM"""
        if self.DATA_ENV == "DUKE_2" and source_config.get("encounter_join_col"):
            source_df = self.get_encounter_joined_dtm(source_df, source_config.get("encounter_join_col"), version)
        else:
            source_df = source_df.withColumn("NFER_DTM", F.coalesce(source_df[dtm_name], F.array_min(F.array(timestamp_fields))))

        source_df = self.calc_nfer_age_for_nfer_dtm(source_df)
        return source_df

    def dim_dk_hash_to_int(self, source, source_df: DataFrame):
        hash_config = self.SYSTEM_DICT["hash_tables"]["nfer_hash"].get(source.lower(), {})
        for dim_hash, dim_hash_fields in hash_config.items():
            if dim_hash in ['fact_hash']:
                continue
            print(f'{dim_hash} FIELDS: {dim_hash_fields}')
            dim_hash_config = self.SYSTEM_DICT["hash_tables"]["hash_maps"][dim_hash]
            dim_hash_field = dim_hash_config["nfer_schema"][1]

            dim_hash_map_df = self.read_dim_hash_maps(dim_hash_config["dir_name"])

            for field in dim_hash_fields:
                source_df = source_df.join(F.broadcast(dim_hash_map_df).alias(f"{field}_map"),
                                           source_df[field] == F.col(f"{field}_map.{dim_hash_field}_HASH"),
                                           "left"
                                           ).drop(field)
                source_df = source_df.withColumn(field,
                                                 F.coalesce(F.col(f"{field}_map.{dim_hash_field}_ID"), F.lit(None)))
                source_df = source_df.drop(f"{dim_hash_field}_ID").drop(f"{dim_hash_field}_HASH")
        return source_df

    def addon_fact_hash_fields(self, source, source_df: DataFrame) -> DataFrame:
        hash_config = self.SYSTEM_DICT["hash_tables"]["nfer_hash"].get(source.lower())
        if not hash_config:
            return source_df
        for field in hash_config.get('fact_hash', []):
            source_df = source_df.withColumn(field, F.xxhash64(F.col(field)))

        return source_df

    def addon_extraction_fields(self, source_df: DataFrame, table):
        if table not in self.SYSTEM_DICT["extraction_jobs"]["tables"]:
            return source_df, [], None

        add_ons = []
        latest_meta = {}
        extraction_configs = self.extraction_configs["tables"][table]["extractions"]
        for extraction in extraction_configs:
            if extraction_configs[extraction]["table_type"] != "join_table":
                continue
            latest_harmonized_dir, latest_meta = self.locate_harmonization_dir(table, extraction)
            add_ons.extend(extraction_configs[extraction]["add_ons"])
            join_fields = extraction_configs[extraction]["join_fields"]
            useful_cols_set = set(join_fields).union(set(extraction_configs[extraction]["add_ons"]))

            harmonization_output_dir = None
            if latest_harmonized_dir:
                harmonization_output_dir = os.path.join(latest_harmonized_dir, self.harmonized_out_dir, extraction.upper())
                harmonized_df = self.read_parquet_to_df(harmonization_output_dir)
            else:
                harmonized_df = self.create_empty_dataframe(list(useful_cols_set), ["STRING" for i in useful_cols_set])
            harmonized_df = harmonized_df.dropDuplicates(extraction_configs[extraction]["join_fields"])

            delim = " nferdelimnfer " if table in ['FACT_DIAGNOSIS'] else ","
            for column in harmonized_df.schema.fields:
                if column.dataType.simpleString().upper() == "STRING":
                    harmonized_df = self.normalize_extraction_field(harmonized_df, column.name, delim)

            """
            df_cols = source_df.columns
            map_cols = harmonized_df.columns
            new_addon_cols = list(set(map_cols).difference(set(join_fields))) # this is what we deserve

            columns_to_be_replaced=list(set(df_cols).intersection(set(new_addon_cols)))

            if replace_previous_addon_cols: # if any addon cols existed in the dataframe, replace it
                print(f"Replacing existing cols: {columns_to_be_replaced}")
                source_df = source_df.drop(*columns_to_be_replaced)
            else:
                if len(columns_to_be_replaced)>0:
                    print(f"Not replacing prev cols: {columns_to_be_replaced} ")
            """

            harmonized_df = harmonized_df.select(list(useful_cols_set))
            for col in join_fields:
                harmonized_type = harmonized_df.schema[col].dataType
                source_type = source_df.schema[col].dataType
                if harmonized_type != source_type:
                    print(f"Casting {col} from {harmonized_type} to {source_type}")
                    harmonized_df = harmonized_df.withColumn(col, F.col(col).cast(source_type))

            use_broadcast = True
            if latest_harmonized_dir:
                harmonization_output_size = self.get_dir_size_in_gb(harmonization_output_dir)
                use_broadcast = harmonization_output_size < 1
            source_df = Orchestrator.join_with_null(source_df, harmonized_df, join_fields, "left", use_broadcast)

        return source_df, add_ons, latest_meta.get("tag")

    def increment_row_id(self, source: str, source_df: DataFrame,version=None):
        if version:
            data_dir = os.path.join(self.WRITE_ROOT_DIR, version)
        else:
            data_dir = self.DATA_DIR
        fact_maps_dir = os.path.join(data_dir, "MAX_ROW_ID", source)
        if not self.glob(fact_maps_dir):
            return source_df
        fact_maps_df = self.read_parquet_to_df(fact_maps_dir)
        source_df = source_df.join(F.broadcast(fact_maps_df), "FILE_ID", "LEFT").fillna(0, "MAX_ROW_ID")
        source_df = source_df.withColumn("ROW_ID", F.col("ROW_ID") + F.col("MAX_ROW_ID")).drop("MAX_ROW_ID")
        return source_df

    def generate_genomic_data(self):
        source = 'FACT_GENOMIC_BIOSPECIMEN_FLAGS'
        nfer_schema = self.read_schema(source)[1]
        self.init_spark_session(source)

        raw_source_file = "/data/RAW_DATA_FACT_GENOMIC/FACT_GENOMIC_4.007/genomic_4.007.txt"
        raw_schema = self.build_source_amc_schema(source, raw_source_file, self.RUN_VERSION)
        source_df = self.read_csv_to_df(raw_source_file, raw_schema)
        source_df = source_df.withColumn("FLAG",
            F.when(F.col("PROJECT_NAME").startswith("Generation"), "Generation")
            .when(F.col("PROJECT_NAME").startswith("Tapestry"), "Tapestry")
            .when(F.col("PROJECT_NAME").startswith("Cirrhosis"), "Cirrhosis")
            .when(F.col("PROJECT_NAME").startswith("psc_pbc"), "PSC_PBC")
            .otherwise("UNKNOWN"))

        source_df.cache()
        print("Raw Source Rows:", source_df.count())
        source_df.groupBy("FLAG").agg(
            F.count("*").alias("COUNT"),
            F.countDistinct(self.PATIENT_CLINIC_NBR).alias("PATIENT_CLINIC_NUMBER_COUNT")
        ).show(truncate=False)

        dim_maps_df = self.read_versioned_dim_maps([self.PATIENT_CLINIC_NBR, "NFER_PID"]).distinct()
        source_df = source_df.join(F.broadcast(dim_maps_df), [self.PATIENT_CLINIC_NBR], "left").na.fill(0, ["NFER_PID"])

        source_df = self.assign_file_id_for_nfer_pid(source_df, source)
        source_df = self.generate_row_id(source_df)
        source_df.cache()
        print("Final Rows:", source_df.count())
        source_df.groupBy("FLAG").agg(
            F.count("*").alias("COUNT"),
            F.countDistinct("NFER_PID").alias("NFER_PID")
        ).show(truncate=False)

        datagen_dir = os.path.join(self.WRITE_ROOT_DIR, self.RUN_VERSION, "DATAGEN", source)
        self.df_to_patient_blocks(source_df, datagen_dir, nfer_schema, source)

        delta_df = source_df.select(nfer_schema)
        delta_df = delta_df.withColumn("VERSION", F.lit(float(self.RUN_VERSION)))
        delta_df = delta_df.withColumn("UPDATED_BY", F.lit(0.0))

        delta_table_path = os.path.join(self.WRITE_ROOT_DIR, self.RUN_VERSION, "DELTA_TABLES/FACT_TABLES", f"{source}.parquet")
        self.write_df_to_delta_lake(delta_df, delta_table_path)


    def calc_stats(self, source_df:DataFrame, extraction_fields):
        counter = Counter()
        source_df = source_df.withColumn("total_fact_rows", F.lit(1))
        source_df = source_df.withColumn("zero_pid_rows", F.when(F.col("NFER_PID").isin(0), 1).otherwise(0))
        source_df = self.capture_dtm_stats(source_df)
        source_df = self.capture_empty_extractions(source_df, extraction_fields)

        stats_fields = ["total_fact_rows", "nfer_dtm_null", "nfer_dtm_future", "zero_pid_rows", "negative_age", "empty_age", "90_plus_age", "empty_extraction"]
        stats_fields = [field for field in stats_fields if field in source_df.columns]
        stats_df = source_df.groupBy().agg(*[F.sum(field).alias(field) for field in stats_fields])
        for row in stats_df.collect():
            if row["total_fact_rows"]:
                for field in stats_fields:
                    counter.update({field: row[field]})

        return counter


    def capture_dtm_stats(self, source_df:DataFrame):
        if "NFER_DTM" in source_df.columns:
            source_df = source_df.withColumn("nfer_dtm_null", F.when(F.col("NFER_DTM").isNull(), 1).otherwise(0))
            source_df = source_df.withColumn("nfer_dtm_future", F.when(F.col("NFER_DTM") > self.this_year_epoch, 1).otherwise(0))
            source_df = source_df.withColumn("negative_age", F.when((F.col("NFER_AGE") < 0), 1).otherwise(0))
            source_df = source_df.withColumn("empty_age", F.when(F.col("NFER_AGE").isNull(), 1).otherwise(0))
            source_df = source_df.withColumn("90_plus_age", F.when((F.col("NFER_AGE") > 90), 1).otherwise(0))
        else:
            default_fields = ["nfer_dtm_null", "nfer_dtm_future", "negative_age", "empty_age", "90_plus_age"]
            for field in default_fields:
                source_df = source_df.withColumn(field, F.lit(0))
        return source_df


    def capture_empty_extractions(self, source_df: DataFrame, extraction_fields):
        for field in extraction_fields:
            if "empty_extraction" not in source_df.columns:
                source_df = source_df.withColumn("empty_extraction",
                    F.when(F.col(field).isNull(), F.lit(1)).otherwise(F.lit(0)))
            else:
                source_df = source_df.withColumn("empty_extraction",
                    F.when(F.col("empty_extraction").isin(1) & F.col(field).isNull(), F.lit(1)).otherwise(F.lit(0)))
        return source_df


    def validate_stats(self, source, counter):
        fields_to_validate = ['zero_pid_rows', 'invalid_age', 'empty_extraction', 'nfer_dtm_null', 'nfer_dtm_future']
        for field in fields_to_validate:
            field_percent = round(counter[field] / max(counter["total_fact_rows"], 1) * 100, 2)
            if field_percent > self.MAX_THRESHOLD:
                if not self.SAMPLE_RUN:
                    raise Exception(f"ERROR: {source}.{field} has {field_percent}% of the total rows.")


    def validate_fact_counts(self, source, counter,version=None):
        if not version:
            data_dir = self.DATA_DIR
        else:
            data_dir = os.path.join(self.WRITE_ROOT_DIR, version)
        pre_processed_stat_file = os.path.join(data_dir, "STATS", f"{source.lower()}.json")
        if self.glob(pre_processed_stat_file):
            pre_processed_stats = self.get_json_data(pre_processed_stat_file)
            pre_processed_count = pre_processed_stats["OUTPUT"]["TOTAL"]
            current_count = counter["total_fact_rows"]
            if current_count != pre_processed_count:
                raise Exception(f"ERROR: {source} has {current_count} rows. Pre-processed count was {pre_processed_count}.")


    def generate_patient_map_json(self):
        total_file_count = self.MAX_BUCKETS * self.MAX_SUB_BUCKETS
        sample_file_count = 0.1 * total_file_count
        total_patient_map = {}
        sample_patient_map = {}
        PATIENT_PER_FILE = 10000
        for i in range(total_file_count):
            id = i + 1
            start = i * self.MAX_BUCKETS + 1
            fact_file_id = '%07d' % (start)
            dim_file_id = '%07d' % (int(start / PATIENT_PER_FILE) * PATIENT_PER_FILE + 1)
            block = '%07d' % ((start // (self.MAX_PATIENTS // self.MAX_PARTITIONS))*(self.MAX_PATIENTS // self.MAX_PARTITIONS))

            key = f'patient_{block}_{fact_file_id}.final'
            dim_patient = f'patient_{block}_{dim_file_id}.final'

            total_patient_map[key] = {
                'id': id,
                'block': block,
                'start': start,
                'dim patient': dim_patient
            }
            if i < sample_file_count:
                sample_patient_map[key] = {
                    'id': id,
                    'block': block,
                    'start': start,
                    'dim patient': dim_patient
                }

        os.makedirs(os.path.join(self.DATAGEN_DIR, "DIM_MAPS"), exist_ok=True)
        outfile = os.path.join(self.DATAGEN_DIR, "DIM_MAPS", "patient_map.json")
        self.dump_json_data(outfile, total_patient_map)
        print(f'Patient map file generated at {outfile}')
        sample_outfile = os.path.join(self.DATAGEN_DIR, "DIM_MAPS", "sample_patient_map.json")
        self.dump_json_data(sample_outfile, sample_patient_map)
        print(f'Sample patient map file generated at {sample_outfile}')


    def upload_harmonized_interim_misc_data(self, source_list):
        for table in source_list:
            if table not in self.SYSTEM_DICT["extraction_jobs"]["tables"]:
                continue
            for extraction in self.extraction_configs["tables"][table]["extractions"]:
                latest_harmonized_dir, _ = self.locate_harmonization_dir(table, skip_local_dir=True)
                local_harmonized_dir = os.path.join(self.LOCAL_DATA_DIR, "HARMONIZED_INTERIM", table, latest_harmonized_dir.split("/")[-2])
                print(f"local_harmonized_dir: {local_harmonized_dir}")
                if os.path.exists(local_harmonized_dir) and latest_harmonized_dir.startswith("gs://"):
                    print(f"Uploading harmonized data from {local_harmonized_dir} to {latest_harmonized_dir}\n")
                    try:
                        self.upload_dir(local_harmonized_dir, latest_harmonized_dir)
                    except Exception as e:
                        # This is to handle the case where we intentionally skipped doing some extractions
                        print(f"Error {e} when uploading {local_harmonized_dir}")


    def duplicate_old_fields(self, source, source_df:DataFrame) -> DataFrame:
        if 'NFER_DRUG' in source_df.columns:
            source_df = source_df.withColumn("DRUG_NAMES", F.col("NFER_DRUG"))
        if 'NFER_DRUG_CLASSES' in source_df.columns:
            source_df = source_df.withColumn("DRUG_CLASSES", F.col("NFER_DRUG_CLASSES"))
        if source == 'FACT_DIAGNOSIS':
            new_cols = [
                "NFER_ONLY_DIAGNOSIS",
                "NFER_FULL_EXTRACTION",
                "NFER_ENRICHED_DIAGNOSIS",
                "NFER_ENRICHED_FULL_EXTRACTION"
            ]
            prev_cols = [name[5:] for name in new_cols]
            print(f"Copying columns {new_cols} to {prev_cols} for {source}")
            for new,prev in zip(new_cols,prev_cols):
                source_df = source_df.withColumn(prev, F.col(new))
        return source_df

    def gen_spark_sql_delta_tables(self, source_list):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        print("Generating FACT Delta Tables for ", version)
        delta_table_root_dir = os.path.join(self.WRITE_ROOT_DIR, version, "DELTA_TABLES")

        for source in source_list:
            print(f'SOURCE: {source}')
            if not self.data_dir("DATAGEN", source, run_version=version):
                print("No source file found for ", source)
                continue

            delta_table_path = os.path.join(delta_table_root_dir, "FACT_TABLES", f"{source.upper()}.parquet")
            if self.skip_if_exists and self.glob(os.path.join(delta_table_path, "_delta_log", "*.json")):
                print(f"Already exists, hence skipping {delta_table_path}")
                continue

            start = time.perf_counter()
            self.init_spark_session(source)

            fact_map_fields = ["FACT_GUID", "VERSION", "UPDATED_BY"]
            if source in self.SIGNAL_TABLES:
                fact_map_fields = ["FACT_GUID", "FACT_ID", "VERSION", "UPDATED_BY"]
            final_schema: list = self.read_datagen_schema(source) + fact_map_fields
            if source == "FACT_ECG_WAVEFORM":
                final_schema.remove("WAVEFORM")
            if source == "FACT_LAB_TEST":
                final_schema.remove("NFER_WITHIN_BOUNDS")
                final_schema.remove("NFER_BOUNDS_TYPE")
            print(f'this is final schema: {final_schema}')
            source_df = self.read_versioned_datagen_dir(source, columns=final_schema, latest_only=False, version=version, force_csv_read=True)
            print('line 463')
            print(f'this is columns for source_df: {source_df.columns}')
            source_df = source_df.drop("FILE_ID")


            # 42 was previously null
            hash_columns = self.SYSTEM_DICT["hash_tables"]["nfer_hash"].get(source.lower(), {}).get('fact_hash', [])
            if hash_columns:
                for _col in hash_columns:
                    source_df = source_df.withColumn(_col, F.when(F.col(_col) != 42, F.col(_col)).otherwise(F.lit(None)))

            self.release_versioned_delta_table(source_df, source, version, write_latest_delta=True)
            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("SPARK_SQL: Total Time =", end - begin)

    def parquet_to_final(self, source_list):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        for source in source_list:
            print(f'SOURCE: {source}')
            source_dir = os.path.join(self.ROOT_DIR, version, "DATAGEN.parquet", source)
            if not self.glob(source_dir):
                print("No source file found for ", source)
                continue

            out_dir = os.path.join(self.LOCAL_ROOT_DIR, version, "DATAGEN", source)
            if self.skip_if_exists and self.glob(os.path.join(out_dir, "header.csv")):
                print(f"Already exists, hence skipping {source}")
                continue

            start = time.perf_counter()
            self.init_spark_session(source)
            source_df = self.read_parquet_to_df(source_dir)
            self.df_to_patient_blocks(source_df, out_dir, source_df.columns, source)

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("PARQUET_TO_FINAL: Total Time =", end - begin)

    def parquet_to_final_bucket(self, source_list):
        begin = time.perf_counter()
        version = self.options.version if self.options.version else self.RUN_VERSION
        source = 'FACT_ECG_WAVEFORM'
        print(f'SOURCE: {source}')
        source_dir = os.path.join(self.ROOT_DIR, version, "DATAGEN.parquet", source)
        if not self.glob(source_dir):
            print("No source file found for ", source)
        out_dir = os.path.join(self.ROOT_DIR, version, "DATAGEN", source)
        if self.skip_if_exists and self.glob(os.path.join(out_dir, "header.csv")):
            print(f"Already exists, hence skipping {source}")

        start = time.perf_counter()
        self.init_spark_session(source)
        source_df = self.read_parquet_to_df(source_dir)
        self.df_to_patient_blocks(source_df, out_dir, source_df.columns, source)

        end = time.perf_counter()
        print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("PARQUET_TO_FINAL: Total Time =", end - begin)


if __name__ == '__main__':
    FactGenJob().run()
