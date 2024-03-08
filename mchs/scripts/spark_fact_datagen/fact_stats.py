import os
import glob
import time
import datetime
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.ml.feature import Bucketizer
from spark_jobs.orchestrator import Orchestrator
from core.config_vars import RunMode


INVALID_TIMESTAMP = datetime.datetime(year=1800, month=1, day=1).timestamp()
INVALID_DATE = "1800-01-01"
INVALID_LIST = { '', 'None', None }


class FactStatsGenJob(Orchestrator):

    def __init__(self):
        super().__init__()
        self.parent = self
        self.bucket_size_list = self.get_log_buckets(self.MAX_PATIENTS)
        self.dk_func_dict = {
            'FACT_DIAGNOSIS' : self.get_diagnosis_code_info,
            'FACT_PROCEDURES' : self.get_procedure_code_info,
            'FACT_LAB_TEST' : self.get_lab_test_loinc_info,
            'FACT_MEDS_ADMINISTERED' : self.get_medicine_info,
            'FACT_ORDERS' : self.get_order_info,
            'FACT_ECHO_TEST' : self.get_echo_info
        }


    def run(self):
        table_list = self.parse_options_source()
        if 'uniques' == self.options.run:
            self.bucket_counts(table_list)
        if 'dk_counts' == self.options.run:
            self.table_dk_field_counts(table_list)
        if 'enc_counts' == self.options.run:
            self.encounter_counts(table_list)
        if 'enc_count_dk_info' == self.options.run:
            self.catalog_field_encounters(table_list)
        if 'y_row_counts' == self.options.run:
            self.gen_year_wise_distribution(table_list)


    def get_log_buckets(self, max_size):
        range_list = []
        c = 1
        while c < max_size:
            range_list.append(c)
            c *= 10
        base_list = [ i * 10 for i in range(1, 11) ]
        bucket_list = [ 1, 5 ]
        for r in range_list:
            bucket_list.extend([ i * r for i in base_list ])
        bucket_size_list = sorted(set(bucket_list))
        return bucket_size_list


    def rename_to_catalog_format(self, source_nick_name, encounter_dir):
        encounter_files = self.glob(os.path.join(encounter_dir, "part-*.csv"))
        for file in encounter_files:
            file_id = os.path.basename(file).split('-')[1][3:5]
            new_file = os.path.join(encounter_dir, f"encounter_{source_nick_name.lower()}_{file_id}.txt")
            self.rename_file(file, new_file)


    def consolidate_bucket_counts(self, run_version):
        consolidated_counts = {}
        uniques_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, 'UNIQUES')
        bucket_count_files = glob.glob(os.path.join(uniques_dir, "bucket_count_fact_*.json"))
        for file in bucket_count_files:
            fact_counts = self.get_json_data(file)
            consolidated_counts.update(fact_counts)
        consolidated_file = os.path.join(uniques_dir, "bucket_counts.json")
        self.dump_json_data(consolidated_file, consolidated_counts)


    def bucket_counts(self, source_list):
        begin = time.perf_counter()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        stats_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, 'STATS')
        uniques_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, 'UNIQUES')
        total_counts_file = os.path.join(stats_dir, "total_counts.json")
        total_counts = {}
        if self.glob(total_counts_file):
            total_counts = self.get_json_data(total_counts_file)

        for source in source_list:
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            if not self.data_dir("DATAGEN", source, run_version=run_version):
                print(f"NO DATA FOR {source}")
                continue

            bucket_counts_file = os.path.join(uniques_dir, f"bucket_count_{source.lower()}.json")
            if self.skip_if_exists and self.glob(bucket_counts_file):
                continue

            #READ DATA
            self.init_spark_session(source)
            raw_source_df = self.read_versioned_datagen_dir(source, ["NFER_PID"], version=run_version)
            if not raw_source_df:
                print(f"NO DATA FOR {source}")
                continue

            #TOTAL COUNT
            raw_source_df.cache()
            total_count = raw_source_df.count()
            total_counts[source] = total_count

            #BUCKET COUNT
            source_df = raw_source_df.groupBy("NFER_PID").count()
            source_df = source_df.withColumnRenamed("count", "RAW_COUNT")
            bucketizer = Bucketizer(splits=self.bucket_size_list, inputCol="RAW_COUNT", outputCol="BUCKET_INDEX").setHandleInvalid("keep")
            source_df = bucketizer.transform(source_df)
            source_df = source_df.groupBy("BUCKET_INDEX").count()
            source_df = source_df.withColumnRenamed("count", "BUCKET_COUNT")
            source_df = source_df.collect()

            #WRITE DATA
            bucket_data = {}
            for row in source_df:
                bucket = self.bucket_size_list[int(row['BUCKET_INDEX'])]
                bucket_data[bucket] = row['BUCKET_COUNT']

            raw_source_df.unpersist()
            self.dump_json_data(bucket_counts_file, bucket_data)
            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        self.consolidate_bucket_counts(run_version)
        self.dump_json_data(total_counts_file, total_counts)
        end = time.perf_counter()
        print("BUCKETS: Total Time =", end - begin)


    def table_dk_field_counts(self, source_list):
        begin = time.perf_counter()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        for source in source_list:
            start = time.perf_counter()

            #READ DATA + CONFIG
            table_dk_config = self.SYSTEM_DICT['data_tables'][source.lower()].get('bucket_files')
            if not table_dk_config:
                continue

            print(f'SOURCE: {source}')
            if not self.glob(os.path.join(self.DATAGEN_DIR, source)):
                print(f"NO DATA FOR {source}")
                continue

            self.init_spark_session(source)
            dk_field = (list(table_dk_config.keys()))[0]
            uniques_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, 'UNIQUES')
            os.makedirs(uniques_dir, exist_ok=True)
            table_dk_count_file_name = os.path.join(uniques_dir, table_dk_config[dk_field])

            if self.options.category == RunMode.Full.value:
                source_df = self.read_versioned_datagen_dir(source, ["NFER_PID", dk_field], version=run_version)
                if not source_df:
                    print(f"NO DATA FOR {source}")
                    continue
            else:
                source_df = self.read_source_files(source, version=run_version)
                if not source_df:
                    print(f"NO DATA FOR {source}")
                    continue
                source_df = source_df.select(self.PATIENT_DK, dk_field)
                patient_map_df = self.read_versioned_dim_maps([self.PATIENT_DK, "NFER_PID"])
                source_df = source_df.join(F.broadcast(patient_map_df), [self.PATIENT_DK], "LEFT")
                source_df = source_df.na.fill(0, ["NFER_PID"])

            #COUNT
            source_df = source_df.groupBy(dk_field).agg(
                F.count("NFER_PID").alias("COUNT"),
                F.countDistinct("NFER_PID").alias("UNIQUE")
            ).collect()

            #WRITE DATA
            with open(table_dk_count_file_name, 'w') as f:
                f.write('Value,Count,Unique\n')
                for row in source_df:
                    f.write(f'{row[dk_field]},{row["COUNT"]},{row["UNIQUE"]}\n')

            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("TABLE DK FIELDS: Total Time =", end - begin)


    def gen_year_wise_distribution(self, source_list):
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        print("RUN_VERSION=",run_version)
        print("PATH=",os.path.join(self.ROOT_DIR,run_version))
        if(not self.check_file_exists(os.path.join(self.ROOT_DIR,run_version))):
            return
        
        nfer_schema = ["NFER_PID", "TOTAL_COUNT", "DATE_WITH_COUNT"]
        type_schema = ["INTEGER", "INTEGER", "STRING"]

        self.init_spark_session()
        empty_df = self.create_empty_dataframe(nfer_schema, type_schema)

        stats_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, "STATS")
        stats_file = os.path.join(stats_dir, "y_row.json")
        print("OUTPUT_PATH=",stats_file)
        if self.skip_if_exists and self.glob(stats_file):
            print(f"SKIPPING {stats_file}")
            return

        y_row_data = {"ALL": {}}
        for source in source_list:
            print(f"SOURCE: {source}")

            source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
            if not source_config.get('dtm'):
                continue

            if source_config.get('nick_name'):
                source_nick_name = f"FACT_{source_config['nick_name']}"
            else:
                source_nick_name = source

            encounter_dir = os.path.join(self.ROOT_DIR,run_version,"COUNTS",source_nick_name)
            if not self.check_file_exists(encounter_dir):
                print("NO DATA FOR", source_nick_name, "at", encounter_dir)
                encounter_dir = self.last_data_dir("COUNTS", source_nick_name, run_version=run_version)

            if not encounter_dir:
                continue

            print("DIRECTORY=",encounter_dir)
            source_files = self.glob(os.path.join(encounter_dir, "*.txt"))
            if not source_files:
                print(f"NO DATA FOR {source}")
                continue

            source_df = self.read_csv_to_df(source_files, empty_df.schema, header=False, sep='|')

            source_df=source_df.withColumn("DATES", F.split(F.col("DATE_WITH_COUNT"), ",")).withColumn("DATE",F.explode(F.col("DATES"))).withColumn("Splitted",F.split(F.col("DATE"),":"))
            source_df=source_df.select("NFER_PID",source_df.Splitted[0],source_df.Splitted[1])
            source_df=source_df.withColumn("YEAR",F.substring(F.col("Splitted[0]"), 1, 4).cast("INTEGER"))
            source_df=source_df.withColumnRenamed("Splitted[1]","TOTAL_COUNT")

            source_df = source_df.filter(F.col("YEAR").between(1900, 2075))
            source_df = source_df.select("NFER_PID", "YEAR", "TOTAL_COUNT")
            temp_df = source_df.groupBy("YEAR").agg(
                F.sum("TOTAL_COUNT").alias("ROW_COUNT"),
                F.countDistinct("NFER_PID").alias("TOTAL_PATIENT_COUNT")
            )
            source_df=source_df.groupby("NFER_PID").agg(F.min("YEAR").alias("FIRST_YEAR"))
            source_df=source_df.groupby("FIRST_YEAR").agg(F.count("NFER_PID").alias("PATIENT_COUNT"))
            source_df=temp_df.join(source_df,temp_df.YEAR ==  source_df.FIRST_YEAR,"full")
            source_df=source_df.fillna(0,["PATIENT_COUNT"])

            source_df= source_df.withColumn("ROW_COUNT",F.col("ROW_COUNT").cast("long"))

            src_y_row = y_row_data.setdefault(source, {})
            for row in source_df.collect():
                src_y_row[row["YEAR"]] = {
                    "ROW_COUNT": row["ROW_COUNT"],
                    "PATIENT_COUNT": row["PATIENT_COUNT"],
                    "TOTAL_PATIENT_COUNT": row["TOTAL_PATIENT_COUNT"]
                }

                src_all_row = src_y_row.setdefault(0, {})
                src_all_row["ROW_COUNT"] = src_all_row.get("ROW_COUNT", 0) + row["ROW_COUNT"]
                src_all_row["PATIENT_COUNT"] = src_all_row.get("PATIENT_COUNT", 0) + row["PATIENT_COUNT"]
                src_all_row["TOTAL_PATIENT_COUNT"] = src_all_row.get("TOTAL_PATIENT_COUNT", 0) + row["TOTAL_PATIENT_COUNT"]

                all_y_row = y_row_data["ALL"].setdefault(row["YEAR"], {})
                all_y_row["ROW_COUNT"] = all_y_row.get("ROW_COUNT", 0) + row["ROW_COUNT"]
                all_y_row["PATIENT_COUNT"] = all_y_row.get("PATIENT_COUNT", 0) + row["PATIENT_COUNT"]
                all_y_row["TOTAL_PATIENT_COUNT"] = all_y_row.get("TOTAL_PATIENT_COUNT", 0) + row["TOTAL_PATIENT_COUNT"]

                all_all_row = y_row_data["ALL"].setdefault(0, {})
                all_all_row["ROW_COUNT"] = all_all_row.get("ROW_COUNT", 0) + row["ROW_COUNT"]
                all_all_row["PATIENT_COUNT"] = all_all_row.get("PATIENT_COUNT", 0) + row["PATIENT_COUNT"]
                all_all_row["TOTAL_PATIENT_COUNT"] = all_all_row.get("TOTAL_PATIENT_COUNT", 0) + row["TOTAL_PATIENT_COUNT"]

        sorted_y_row_data = {}
        for source, y_row in y_row_data.items():
            sorted_y_row_data[source] = {}
            for year in range(1900, 2075):
                year_val = str(year) if year > 0 else "ALL"
                sorted_y_row_data[source][year_val] = y_row.get(year, {
                    "ROW_COUNT": 0,
                    "PATIENT_COUNT": 0,
                    "TOTAL_PATIENT_COUNT":0
                })

        self.dump_json_data(stats_file, sorted_y_row_data)
        print("YEAR WISE DISTRIBUTION: ", stats_file)


    def encounter_counts(self, source_list):
        begin = time.perf_counter()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        for source in source_list:
            source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
            if source_config.get('nick_name'):
                source_nick_name = f"FACT_{source_config['nick_name']}"
            else:
                source_nick_name = source
            if "NFER_DTM" not in source_config["nfer_schema"]:
                continue

            out_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, "COUNTS", source_nick_name)
            if self.skip_if_exists and self.glob(out_dir):
                continue

            #READ DATA
            start = time.perf_counter()
            print(f'SOURCE: {source}')
            if not self.data_dir("DATAGEN", source, run_version=run_version):
                print(f"NO DATA FOR {source}")
                continue

            self.init_spark_session(source)
            if source_config.get('nick_name', source) == "PATHOLOGY":
                source_df = self.read_versioned_datagen_dir(source, ["NFER_PID", "NFER_DTM","PATHOLOGY_FPK","LOCATION_SITE_NAME"], version=run_version)
                source_df = source_df.filter(F.col("LOCATION_SITE_NAME")=="RST")
                print("before",source_df.count())
                source_df = self.modify_pathology_df(source_df,run_version)
            else:
                source_df = self.read_versioned_datagen_dir(source, ["NFER_PID", "NFER_DTM"], version=run_version)
            if not source_df:
                print(f"NO DATA FOR {source}")
                continue

            #COUNT
            source_df = source_df.filter(F.col("NFER_DTM").isNotNull())
            source_df = source_df.withColumn("DATE", F.from_unixtime(F.col("NFER_DTM"), format="yyyy-MM-dd"))
            source_df = source_df.groupBy("NFER_PID", "DATE").agg(F.count("*").alias("DATE_COUNT"))
            source_df = source_df.withColumn("DATE_WITH_COUNT", F.concat_ws(":", F.array(["DATE", "DATE_COUNT"])))
            source_df = source_df.repartition(self.num_files, "NFER_PID")
            source_df = source_df.groupBy("NFER_PID").agg(
                F.sum("DATE_COUNT").alias("TOTAL_COUNT"),
                F.concat_ws(",", F.collect_list("DATE_WITH_COUNT")).alias("DATE_WITH_COUNT")
            )

            #WRITE DATA
            source_df = source_df.select(["NFER_PID", "TOTAL_COUNT", "DATE_WITH_COUNT"])
            source_df = source_df.repartition(16, "NFER_PID")
            source_df.write.csv(out_dir, sep='|', header=False, mode="overwrite")
            self.rename_to_catalog_format(source_nick_name, out_dir)
            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("ENCOUNTERS: Total Time =", end - begin)

    def modify_pathology_df(self,source_df,run_version):
        table_name = "FACT_PATHOLOGY_SPECIMEN_DETAIL"
        source_df_specimen = self.read_versioned_datagen_dir(table_name, ["NFER_PID","BLOCK_INSTANCE", "PART_INSTANCE", "PART_DESCRIPTION","PATHOLOGY_FPK"], version=run_version)
        source_df_specimen = source_df_specimen.filter(source_df_specimen.NFER_PID.isNotNull() & source_df_specimen.BLOCK_INSTANCE.isNotNull() & source_df_specimen.PATHOLOGY_FPK.isNotNull() & source_df_specimen.PART_INSTANCE.isNotNull() & source_df_specimen.PART_DESCRIPTION.isNotNull())
        source_df_specimen = source_df_specimen.select(["NFER_PID","PATHOLOGY_FPK"])
        source_df = source_df.join(source_df_specimen,["NFER_PID","PATHOLOGY_FPK"],"inner")
        
        print("after",source_df.count())
        return source_df

    def read_source_with_dk_info(self, source, catalog_fields, run_version):
        fields = ["NFER_PID", "NFER_DTM"] + catalog_fields
        if source == 'FACT_GENOMIC_BIOSPECIMEN_FLAGS':
            fields.remove("NFER_DTM")

        source_df = self.read_versioned_datagen_dir(source, version=run_version, columns=fields)
        if not source_df:
            return None

        dtm_field = next((field for field in catalog_fields if field.endswith("_DTM")), None)
        if dtm_field:
            source_df = source_df.withColumn("NFER_DTM", F.col(dtm_field))

        if source in ['FACT_DIAGNOSIS']:
            source_df = source_df.withColumn("DK", F.col(catalog_fields[0]))
            source_df = source_df.withColumn("DK_INFO", F.col("NFER_ONLY_DIAGNOSIS"))

        elif source in ['FACT_MEDS_ADMINISTERED']:
            source_df = source_df.withColumn("DK", F.col(catalog_fields[0]))
            source_df = source_df.withColumn("DK_INFO", F.col("NFER_DRUG"))

        elif source in ['FACT_ORDERS']:
            source_df = source_df.withColumn("DK", F.col(catalog_fields[0]))
            source_df = source_df.withColumn("DK_INFO", F.col("NFER_DRUG"))
            source_df = source_df.withColumn("ORDER_TYPE_DESCRIPTION", F.col("ORDER_TYPE_DESCRIPTION"))
            li = ['Pharmacy', 'Inpatient Medication Order', 'Medications', 'Prescription Medication Order','Prescription Medication Order - Historical', 'Immunization/Injection','Immunization Medication Order', 'IV', 'Outpatient Medication Order', 'MEDICATION']
            source_df =source_df.filter(source_df.ORDER_TYPE_DESCRIPTION.isin(li))

        elif source in ["FACT_RADIOLOGY"]:
            source_df = source_df.withColumn("DK",
                F.when(F.upper("SERVICE_MODALITY_DESCRIPTION").contains("MAGNETIC RESONANCE"), F.lit("mri"))
                .when(F.upper("SERVICE_MODALITY_DESCRIPTION").contains("COMPUTED TOMOGRAPHY"), F.lit("ct_scan"))
                .otherwise(F.lit(None)))
            source_df = source_df.withColumn("DK_INFO", F.col("DK"))

        elif source in ["FACT_ADMIT_DISCHARGE_TRANSFER_LOCATION"]:
            source_df = self.get_norm_patient_type(source_df, "PATIENT_PROCESS_TYPE")
            source_df = source_df.withColumn("DK", F.col("PATIENT_TYPE"))
            source_df = source_df.withColumn("DK_INFO", F.col("PATIENT_TYPE"))

        elif source in ['FACT_GENOMIC_BIOSPECIMEN_FLAGS']:
            source_df = source_df.withColumn("NFER_DTM", F.lit(INVALID_TIMESTAMP))
            source_df = source_df.withColumn("DK", F.col(catalog_fields[0]))
            source_df = source_df.withColumn("DK_INFO", F.col("DK"))

        elif catalog_fields:
            source_df = source_df.withColumn("DK", F.col(catalog_fields[0]))
            source_df = source_df.withColumn("DK_INFO", F.col("DK"))

        else:
            raise Exception(f'{source} none catalog fields configured')

        source_df = source_df.filter(F.col("DK_INFO").isNotNull())
        source_df = source_df.withColumn("NFER_DTM", F.coalesce(F.col("NFER_DTM"), F.lit(INVALID_TIMESTAMP)))
        return source_df


    def catalog_field_encounters(self, source_list):
        begin = time.perf_counter()
        self.init_spark_session()
        run_version = self.options.version if self.options.version else self.RUN_VERSION
        count_dk_list = [
            "FACT_DIAGNOSIS",
            "FACT_PROCEDURES",
            "FACT_MEDS_ADMINISTERED",
            "FACT_MED_INFUSION",
            "FACT_IMMUNIZATIONS",
            "FACT_ORDERS"
        ]
        count_info_list = [
            "FACT_ADMIT_DISCHARGE_TRANSFER_LOCATION",
            "FACT_GENOMIC_BIOSPECIMEN_FLAGS",
            "FACT_RADIOLOGY"
        ]
        for source in source_list:
            start = time.perf_counter()
            #READ CONFIG
            source_config = self.SYSTEM_DICT['data_tables'][source.lower()]
            if source_config.get('nick_name'):
                source_nick_name = f"FACT_{source_config['nick_name']}"
            else:
                source_nick_name = source
            catalog_fields = source_config.get('catalog_fields')
            if not catalog_fields:
                continue
            if not self.data_dir("DATAGEN", source):
                print(f"NO DATA FOR {source}")
                continue
            if source in count_dk_list:
                out_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, "COUNT_DK", source_nick_name)
            elif source in count_info_list:
                out_dir = os.path.join(self.WRITE_ROOT_DIR, run_version, "COUNT_INFO", source_nick_name)
            else:
                continue
            if self.skip_if_exists and self.glob(out_dir):
                continue

            #READ DATA
            print(f'SOURCE: {source} :', catalog_fields)
            source_df = self.read_source_with_dk_info(source, catalog_fields, run_version)
            if source_df is None:
                print("Skipping source:", source)
                continue
            source_df = source_df.select(["NFER_PID", "NFER_DTM", "DK", "DK_INFO"])

            #TRANSFORM
            source_df = source_df.withColumn("NFER_DATE", F.from_unixtime(F.col("NFER_DTM"), format="yyyy-MM-dd"))
            source_df = source_df.withColumn("NFER_DATE", F.coalesce(F.col("NFER_DATE"), F.lit(INVALID_DATE)))
            source_df = source_df.repartition(self.num_files, "NFER_PID")

            if source in count_dk_list: #COUNT_DK
                count_dk_df = source_df.groupBy("NFER_PID", "NFER_DATE", "DK_INFO").agg(F.count("*").alias("COUNT"))
                count_dk_df = count_dk_df.groupBy("NFER_PID", "DK_INFO").agg(
                    F.sum("COUNT").alias("DK_INFO_COUNT"),
                    F.map_from_arrays(F.collect_list("NFER_DATE"), F.collect_list("COUNT")).alias("DATE_DICT")
                )
                count_dk_df = count_dk_df.groupBy("NFER_PID").agg(
                    F.sum("DK_INFO_COUNT").alias("TOTAL_COUNT"),
                    F.to_json(F.map_from_arrays(F.collect_list("DK_INFO"), F.collect_list("DATE_DICT"))).alias("DK_INFO_DICT")
                )
                count_dk_df = count_dk_df.repartition(16, "NFER_PID")
                count_df = count_dk_df.select(["NFER_PID", "TOTAL_COUNT","DK_INFO_DICT"])

            else: #COUNT_INFO
                count_info_df = source_df.groupBy("NFER_PID", "NFER_DATE", "DK_INFO").agg(F.count("*").alias("COUNT"))
                count_info_df = count_info_df.groupBy("NFER_PID", "DK_INFO").agg(
                    F.sum("COUNT").alias("INFO_COUNT"),
                    F.map_from_arrays(F.collect_list("NFER_DATE"), F.collect_list("COUNT")).alias("DATE_DICT")
                )
                count_info_df = count_info_df.groupBy("NFER_PID").agg(
                    F.sum("INFO_COUNT").alias("TOTAL_COUNT"),
                    F.to_json(F.map_from_arrays(F.collect_list("DK_INFO"), F.collect_list("DATE_DICT"))).alias("INFO_DICT")
                )
                count_info_df = count_info_df.repartition(16, "NFER_PID")
                count_df = count_info_df.select(["NFER_PID", "TOTAL_COUNT", "INFO_DICT"])

            count_df.write.csv(out_dir, sep='|', header=False, mode="overwrite", emptyValue=None, quote="")
            self.rename_to_catalog_format(source_nick_name, out_dir)

            source_df.unpersist()
            end = time.perf_counter()
            print(source, ": Total Time =", end - start)

        end = time.perf_counter()
        print("ENCOUNTERS: Total Time =", end - begin)


if __name__ == '__main__':
    FactStatsGenJob().run()
