import os
from core.config_vars import RunMode
from spark_jobs.orchestrator import Orchestrator
from pyspark.sql import functions as F


class InterimSynDimGenerator(Orchestrator):
    def __init__(self):
        super().__init__()
        self.interim_concept_config = self.SYSTEM_DICT["interim_dim_syn_config"]
        self.init_spark_session()

    def parse_options_source(self):
        if self.options.source == "ALL":
            table_list = []
            for source_table in self.interim_concept_config.keys():
                for dim_syn in self.interim_concept_config[source_table]["dim_syns"].keys():
                    table_list.append(dim_syn)
            return table_list
        else:
            if "," in self.options.source:
                return [table.strip() for table in self.options.source.split(",")]
            return [self.options.source.strip()]

    def set_options(self, parser):
        parser.add_option('--dim_syn_write_path', default = None, help='Path to write the dim_syn ouputs to in Test mode')
        super().set_options(parser)

    def run(self):
        dim_syn_list = self.parse_options_source()
        self.generate_interim_concept_tables(dim_syn_list)

    def read_previous_versions_data(self, source_name):
        previous_version = self.VERSION_LIST[1]
        source_df = self.read_versioned_datagen_table(source_name, previous_version)
        # We need only nfer columns
        _, nfer_schema, _ = self.read_schema(source_name)
        source_df = source_df.select(nfer_schema)
        return source_df

    def read_source_table(self, source_name, read_full=True):
        if source_name in self.SYSTEM_DICT["data_tables"]:
            source_name = source_name.upper()
            if read_full or self.RUN_MODE == RunMode.Full.value:
                # Three cases
                # DIM syn is generated for first time and delta run - Read previous versions delta and union to current DATAGEN
                # Run mode is Full and ees update - Read previous versions delta a
                # Run mode is Full and is base version run - Read current DATAGEN
                if self.ees_full_update():
                    source_df = self.read_previous_versions_data(source_name)
                elif self.RUN_MODE == RunMode.Full.value:
                    # Base version
                    source_df = self.read_incremental_raw_data(source_name)
                else:
                    # To generate dim_syn for the first time
                    source_df_prev = self.read_previous_versions_data(source_name)
                    source_df_current = self.read_incremental_raw_data(source_name)

                    if source_df_current:
                        cols = list(set(source_df_prev.columns).intersection(set(source_df_current.columns)))
                        source_df = source_df_prev.select(cols).unionByName(source_df_current.select(cols))
                    else:
                        # If its an incremental run, but data is not available
                        # from AMC in incremental update, source_df_current will be None
                        source_df = source_df_prev
            else:
                source_df = self.read_incremental_raw_data(source_name)
        elif source_name in self.SYSTEM_DICT["syn_tables"]:
            source_df = self.read_versioned_syn_data(source_name)
        else:
            source_df = self.read_versioned_dim_table(source_name)
        return source_df

    def generate_interim_concept_tables(self, dim_syn_list):
            print(f"Generating tables {dim_syn_list}")
            for source_table in self.interim_concept_config.keys():
                if self.options.run == "ALL":
                    out_dir_root = self.DIM_SYN_INTERIM_DIR
                elif self.options.run == "TEST":
                    if not self.options.dim_syn_write_path:
                        raise Exception(f"Print please provide a path to write the output in argument --dim_syn_write_path")
                    out_dir_root = self.options.dim_syn_write_path
                else:
                    raise Exception("Please specify a run mode, values can be ALL or TEST, argument is --run ")

                source_df = None

                for dim_syn_table in self.interim_concept_config[source_table]["dim_syns"].keys():
                    if dim_syn_table not in dim_syn_list:
                        continue
                    print(f"Processing {dim_syn_table}")
                    output_table_name = self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["output_table"]
                    if self.skip_if_exists and self.glob(os.path.join(out_dir_root, output_table_name, "_SUCCESS")):
                        print(output_table_name, "Already processed, skipping")
                        continue

                    #if dim_syn_table == "DIM_SYN_LAB_TEST" or dim_syn_table == "DIM_SYN_FLOWSHEETS":
                        #last_dim_syn = None
                    #else:
                    last_dim_syn = self.read_last_dim_syn(output_table_name)

                    # If there is dim_syn from previous versions, read only incremental data for incremental run modes
                    # Read entire data if there is no previous dim_syn
                    # We can process by reading entire data also always, but it increases processing time

                    if last_dim_syn and self.ees_full_update():
                        print(f"{dim_syn_table} is present, No need to generate for even version")
                        continue

                    if last_dim_syn:
                        source_df = self.read_source_table(source_table, read_full=False)
                    else:
                        source_df = self.read_source_table(source_table, read_full=True)

                    if not source_df:
                        # No incremental updates
                        if last_dim_syn:
                            out_dir = os.path.join(self.DIM_SYN_INTERIM_DIR, output_table_name.upper())
                            print(f"Writing dim syn to {out_dir}")
                            self.df_to_parquet(last_dim_syn, out_dir)
                            continue
                        else:
                            raise Exception(f"No incremental data or previous dim syn found {dim_syn_table}")

                    # Create DK column(id column) with xxhash
                    source_df = self.add_syn_dk_col(source_df, source_table)


                    # Take distinct of columns required for interim dim and write
                    # for dim_syn_table in self.interim_concept_config[source_table]["dim_syns"].keys():
                    id_column = self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["id_column"]
                    column_for_interim_concept = self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["columns"]
                    columns_for_interim_dim = [id_column] + column_for_interim_concept
                    dim_syn_df = source_df.select(columns_for_interim_dim).distinct()

                    # JOIN with other dims
                    if "dim_tables" in self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]:
                        for table in self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["dim_tables"]:
                            table_config = self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["dim_tables"][table]
                            dim_source_table = table_config["source_table"]
                            dim_df = self.read_source_table(dim_source_table)
                            columns_to_read = \
                            self.interim_concept_config[source_table]["dim_syns"][dim_syn_table]["dim_tables"][table][
                                "columns"]
                            dim_df = dim_df.select(columns_to_read)
                            print(f"Reading {dim_source_table}")
                            dim_df.cache()

                            # Validate that there are no duplicates in joining dim
                            join_fields_dict = table_config["join_key"]
                            join_keys = [join_fields_dict[col] for col in join_fields_dict]
                            validation_df = dim_df.groupBy(join_keys).count()
                            duplicate_records = validation_df.filter(F.col("count") > 1)
                            duplicate_counts = duplicate_records.count()
                            if duplicate_counts > 1:
                                duplicate_records.show()
                                raise Exception("Joined DIM tables has duplicates on join keys")

                            for col in join_fields_dict:
                                dim_df = dim_df.withColumnRenamed(col, join_fields_dict[col])
                            dim_syn_df = dim_syn_df.join(F.broadcast(dim_df), join_keys, "left")

                    if last_dim_syn:
                        # If there is a dim_syn from previous version
                        # Identify new entries and add to previous dim_syn
                        new_dim_syn_ids = dim_syn_df.join(last_dim_syn, id_column, "left_anti").select(id_column)
                        new_dim_syn_df = dim_syn_df.join(new_dim_syn_ids, id_column, "inner")
                        final_dim_syn_df = last_dim_syn.unionByName(new_dim_syn_df)
                    else:
                        final_dim_syn_df = dim_syn_df

                    out_dir = os.path.join(out_dir_root, output_table_name.upper())

                    print(f"Writing dim syn to {out_dir}")
                    self.df_to_parquet(final_dim_syn_df, out_dir)


if __name__ == '__main__':
    InterimSynDimGenerator().run()
