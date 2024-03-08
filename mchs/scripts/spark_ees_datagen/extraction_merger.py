import json
import os
import glob
import time
from collections import Counter
from pyspark.sql import Window, functions as F, DataFrame
from spark_fact_datagen.fact_splicer import FactGenJob
from miscellaneous.commander import *

import logging

log = logging.getLogger(__name__)

class ExtractionMerger(FactGenJob):

    def __init__(self, debug=False) -> None:
        self.debug = debug
        if debug:
            print("** RUNNING with debug on. Will operate on sample data and write to temp directory ***")
        super().__init__()


    def run(self):
        table_list = self.parse_options_source()

        if self.options.debug:
            self.debug = True
        if 'add_new_cols' in self.options.run:
            if len(table_list)>1:
                raise Exception
            source = table_list[0]
            new_cols = self.options.columns
            new_cols = list(map(str.strip,new_cols.split(',')))

            self.add_new_extraction_columns(source,new_cols)
        if 'backfill' in self.options.run:
            for table in table_list:
                self.backfill_harmonized_data(table)
        elif 'full' in self.options.run:
            self.update_harmonized_data(table_list)

    def calc_extraction_hash(self, source_df: DataFrame, extraction_fields, field) -> DataFrame:
        """
        Given a list of columns calculates the col hash. and adds it in the given field name
        normalizes, handles nan, sorts the column and their contents too
        """
        na_indices = []
        for col in extraction_fields:
            source_df = source_df.withColumn(f"{col}_NA", F.col(col))
            source_df = self.normalize_ees_field(source_df, f"{col}_NA")
            na_indices.append(f"{col}_NA")
        source_df = source_df.na.fill(0, na_indices).na.fill("", na_indices)
        source_df = source_df.withColumn(field, F.sha2(F.concat_ws("|", F.array(sorted(na_indices))), 256))
        for col in na_indices:
            source_df = source_df.drop(col)
        return source_df

    def latest_meta_file(self, source):
        # if the extraction doesnt exist in current version, skip
        harmonized_root_dir = os.path.join(self.DATA_DIR, self.harmonized_root_dir, source)
        latest_meta_file = os.path.join(harmonized_root_dir, "HARMONIZED_META", "orchestrator_meta.json")
        if not self.glob(latest_meta_file):
            latest_meta_file = os.path.join(harmonized_root_dir, "latest", "HARMONIZED_META", "orchestrator_meta.json")
        if not self.glob(latest_meta_file):
            print(f'Latest meta file not present in {latest_meta_file} skipping\n')
            return

        # Is this still needed?
        '''if not len(self.extraction_configs["tables"][source]["extractions"]) > 0:
            return # for the stupid ecg header job'''

        return latest_meta_file

    def expected_change_percent(self, source):
        expected_percent = 100

        if self.glob(self.harmonisation_config_file):
            harmonization_config = self.get_json_data(self.harmonisation_config_file)
            for key, table_config in harmonization_config['jobs'].items():
                if table_config["id"] == self.extraction_configs["tables"][source]["id"]:
                    expected_percent = table_config.get("expected_change_percent", 100)
                    break
        else:
            print(f"Harmonization config file {self.harmonisation_config_file} not found. Using default-expected_change_percent = 100%")

        return expected_percent

    def increment_rowid_n_version(self, source, final_df:DataFrame):

        max_row_id_dir = os.path.join(self.DATA_DIR, "MAX_ROW_ID", source)
        max_row_id_df = self.read_parquet_to_df(max_row_id_dir)

        final_df = self.assign_file_id_for_nfer_pid(final_df, source)
        final_df = final_df.join(F.broadcast(max_row_id_df), "FILE_ID")

        rowWindow = Window.partitionBy("FILE_ID").orderBy(F.col("ROW_ID").asc())
        final_df = final_df.withColumn("ROW_ID", F.col("MAX_ROW_ID") + F.row_number().over(rowWindow))
        final_df = final_df.withColumn("VERSION", F.lit(self.RUN_VERSION))
        return final_df

    def validate_extraction_change(self, source, source_orig_size, source_df:DataFrame):
        change_size = source_df.count()
        change_percent = change_size * 100 / source_orig_size
        expected_percent = self.expected_change_percent(source)

        print(f"No of records where data changes = {change_size} This is: {int(change_percent)}% of total data")
        if change_percent > expected_percent:
            raise Exception(f"Change % : {change_percent} is greater than the expected percent : {expected_percent}")

        return change_size, change_percent

    def write_datagen_maps(self, source, source_df: DataFrame):
        datagen_maps_dir = os.path.join(self.WRITE_ROOT_DIR, self.RUN_VERSION, "DATAGEN_MAPS", source)
        datagen_maps_schema = ["NFER_PID", "ROW_ID", "FACT_GUID" ]
        datagen_maps_df = source_df.select(datagen_maps_schema)
        print(f"Writing datagen maps with schema: {datagen_maps_schema} at {datagen_maps_dir}")
        datagen_maps_df.write.parquet(datagen_maps_dir, mode="overwrite", compression="snappy")

    def write_datagen_dir(self, source, source_df: DataFrame):
        final_dir_path = os.path.join(self.DATA_DIR, "DATAGEN", source.upper())
        datagen_schema = self.read_datagen_schema(source)
        print(f"The schema will be {datagen_schema}")
        self.df_to_parquet_blocks(source_df, final_dir_path, datagen_schema, source=source)

    def log_stats(self, source, start, change_size, change_percent, extraction_tag=None):
        counter = Counter()
        counter.update({"change_percent": change_percent})  # TODO
        counter.update({"total_fact_rows": change_size})
        counter.update({"run_time": time.perf_counter() - start})
        counter = dict(counter)
        if extraction_tag:
            counter.update({"extraction_tag": extraction_tag})
        self.log_spark_stats("DATAGEN", source, counter)

    def exists_in_harmonized_interim(self, source):
        path = os.path.join(self.ROOT_DIR, self.RUN_VERSION, "HARMONIZED_INTERIM", source.upper())
        print(f'Checking for file in {path}')
        return self.check_file_exists(path)

    def update_harmonized_data(self, source_list,):
        """
        EES column updates
        before running this make su
        """
        begin = time.perf_counter()
        for source in source_list:

            print(f"\n\n*** Doing data full update for EES extractions in table:{source} ***")
            start = time.perf_counter()
            self.init_spark_session(source)

            if self.skip_if_exists and self.data_dir("DATAGEN", source.upper()):
                print('Output table already exists. Skipping.\n')
                continue

            if not self.exists_in_harmonized_interim(source):
                log.warning(f'Own folder in HARMONIZED_INTERIM dir absent for {source}.. Skipping...')
                continue
            # make sure the addon columns only contain the columns to be versioned
            add_ons = self.read_extraction_schema(source)
            if len(add_ons) == 0:
                print(f'Add on columns for source {source} does not exist. Skipping ')
                continue
            # read all the records with some fact_maps_versioned columns
            source_df = self.read_versioned_datagen_dir(source)

            # skip any newly added columns
            add_ons = [col for col in add_ons if col in source_df.columns]

            if self.debug:
                source_df = source_df.limit(10000)

            source_orig_size = source_df.count()
            print(f"Reading source={source} total active records = {source_orig_size}")



            hash_cols = add_ons
            print(f"Hash will be calculated based on {hash_cols}")
            #calculate the hash of previous addon cols
            source_df = self.calc_extraction_hash(source_df, hash_cols, "OLD_ADDONS_HASH")

            # not needed as prev hash is already calculated
            if not self.debug:
                source_df = source_df.drop(*add_ons)
            else:
                for col in add_ons:
                    source_df = source_df.withColumnRenamed(col,col+'_prev')

            # add extraction fields and

            source_df, _, extraction_tag = self.addon_extraction_fields(source_df, source)

            # Find updates in extractions
            source_df = self.calc_extraction_hash(source_df, hash_cols, "NEW_ADDONS_HASH")
            final_df = source_df.filter((F.col("OLD_ADDONS_HASH") != F.col("NEW_ADDONS_HASH")))

            if not self.debug:
                final_df = final_df.drop("OLD_ADDONS_HASH", "NEW_ADDONS_HASH")

            # Assign new row_ids and version
            final_df = self.increment_rowid_n_version(source, final_df)
            final_df.cache()

            print("final hash values with new addons")
            if self.debug:
                prev_colnames = [col+'_prev' for col in add_ons]
                final_df.select(add_ons+prev_colnames+["NEW_ADDONS_HASH","OLD_ADDONS_HASH"]).show()

            # Validate change percent against expected change percent
            change_size, change_percent = self.validate_extraction_change(source, source_orig_size, final_df)

            self.write_datagen_maps(source, final_df)
            # ToDo: write to parquet
            order_by = None
            if source == "FACT_ECG_WAVEFORM":
                order_by = ["NFER_PID", "NFER_DTM", "WAVEFORM_TYPE", "LEAD_ID"]

            datagen_schema = self.read_final_schema(source)
            self.write_final_dir("DATAGEN", final_df, datagen_schema, source, order_by=order_by)

            self.log_stats(source, start, change_size, change_percent, extraction_tag)
            self.SPARK_SESSION.catalog.clearCache()

        end = time.perf_counter()
        print("EXTRACTION DATAGEN: Total Time =", end - begin)


    def backfill_harmonized_data(self, table,
                                 replace_previous_addon_columns=False):
        """
        # Read all versions of data and update with the new columns

        """
        spark = self.init_spark_session(table,)
        for run_version, run_meta in self.RUN_STATE_LOG.items():
            final_dir_path = os.path.join(self.ROOT_DIR, run_version, "DATAGEN", table)
            if run_meta["STATUS"] != "SUCCESS" or not self.glob(final_dir_path):
                print("Skipping version {}".format(run_version))
                continue

            if self.debug:
                sample_file_path = glob.glob(os.path.join(self.ROOT_DIR, run_version, "DATAGEN", table, "*.final"))[-1:]
                source_df = self.read_final_dir(sample_file_path)
            else:
                source_df = self.read_final_dir(final_dir_path)
            schema = source_df.columns
            print("Merging extractions for {}".format(run_version))
            source_df, add_ons, extraction_tag = self.addon_extraction_fields(source_df, table,
                                                              replace_previous_addon_columns)
            new_additions = [item for item in add_ons if item not in schema]
            schema = schema+new_additions
            print(schema)
            # Writing backfills to separate folder for debugging
            # Replace table(FACT_LAB_TEST) with table_BACKFILL(FACT_LAB_TEST_BACKFILL) after review
            backfill_table = table + "_BACKFILL"
            if self.debug:
                final_dir_path = os.path.join("/data3/tmp", run_version, "DATAGEN", backfill_table)
            else:
                final_dir_path = os.path.join(self.ROOT_DIR, run_version, "DATAGEN",
                                              backfill_table)

            print(f"*** Count of records: version= {run_version} count={source_df.count()} ***")

            self.write_final_dir(source_df, final_dir_path, schema, source=table)

    # A dag for
    def add_new_extraction_columns(self, table, col_list):
        # read latest fact_file_parquet full version

        latest_parquet_dir = os.path.join(self.ROOT_DIR,"CURRENT","DELTA_TABLES", "FACT_TABLES", f"{table.upper()}.parquet")
        bkup_path = latest_parquet_dir+".bkup" if not latest_parquet_dir.endswith('/') else latest_parquet_dir[:-1]+".bkup"
        log.warning(f'Latest parquet resides in {latest_parquet_dir} and bkup_dir {bkup_path}')
        if os.path.exists(latest_parquet_dir):
            if not os.path.exists(bkup_path):
                _,_,ret = run_command(f'mv {latest_parquet_dir} {bkup_path}')
                if ret != 0:
                    raise Exception
            else:
                _,_,ret = run_command(f'rm -rf {latest_parquet_dir}')
                if ret != 0:
                    raise Exception
        else:
            if not os.path.exists(bkup_path):
                log.error(f"Source: {table} parquet does not exist at {latest_parquet_dir} also the backup is missing")
                raise Exception

        self.init_spark_session()
        df = self.read_parquet_to_df(bkup_path)
        extraction_config = self.extraction_configs["tables"][table]["extractions"]

        assert(len(extraction_config) == 1)

        one_extraction = None
        extraction_key_name = None
        for key in extraction_config:
            extraction_key_name = key
            one_extraction = extraction_config[key]

        assert (one_extraction is not None)
        assert (extraction_key_name is not None)

        join_cols = one_extraction['join_fields']
        add_ons = one_extraction['add_ons']
        assert (len(col_list)==len(set(col_list).intersection(add_ons)))

        latest_meta_file = os.path.join(self.ROOT_DIR, "CURRENT", self.harmonized_root_dir, table, "latest",
                                        "HARMONIZED_META", "orchestrator_meta.json")
        print(f'Checking for orchestrator_meta.json only in the CURRENT folder {latest_meta_file}')
        if not os.path.exists(latest_meta_file):
            raise Exception("No Harmonization Meta Found")
        latest_harmonized_dir = os.path.join(os.path.dirname(latest_meta_file), os.pardir)
        harmonization_output_dir = os.path.join(latest_harmonized_dir, self.harmonized_out_dir, extraction_key_name.upper())

        harmonized_df = self.read_parquet_to_df(harmonization_output_dir)
        log.warning(f'Count before dropduplicates on join cols {harmonized_df.count()}')
        harmonized_df = harmonized_df.dropDuplicates(one_extraction["join_fields"])
        hdf_count = harmonized_df.count()
        log.warning(f'Count after dropduplicates on join cols {harmonized_df.count()}')

        delim = " nferdelimnfer " if table in ['FACT_DIAGNOSIS'] else ","
        for column in harmonized_df.schema.fields:
            if column.dataType.simpleString().upper() == "STRING":
                harmonized_df = self.normalize_extraction_field(harmonized_df, column.name, delim)
        join_fields = one_extraction["join_fields"]

        useful_cols_set = set(join_fields).union(set(col_list))
        harmonized_df = harmonized_df.select(list(useful_cols_set))

        use_broadcast = True
        count_before_join = df.count()
        hdf_count_after = harmonized_df.count()
        assert (hdf_count_after == hdf_count)
        df = self.join_with_null(df, harmonized_df, join_fields, "left", use_broadcast)
        count_after_join = df.count()
        print(f"Count before join {count_before_join} ; Count After join: {count_after_join}")
        assert (count_before_join == count_after_join)
        df.show()
        df.printSchema()
        #write parquet
        path = self.release_versioned_delta_table(df,table,"CURRENT", write_latest_delta=True)
        log.warning(f"Wrote the file to {path}")


    # step 2
    def update_csv_files_post_adding_cols(self, table, col_list):
        # read latest parquet

        # filter for each version

        # write back the table initially with different name
        pass


if __name__ == '__main__':
    ExtractionMerger(debug=False).run()
