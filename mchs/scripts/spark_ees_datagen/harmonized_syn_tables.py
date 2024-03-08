import json
import os
from spark_syn_datagen.synthetic_summaries import SynDataGenJob


class HarmonizedSynTables(SynDataGenJob):
    def __init__(self):
        super().__init__()
        self.debug = False
        self.dry_run = False
        self.harmonization_config_dict = None
        with open(self.harmonisation_config_file) as fp:
            self.harmonization_config_dict = json.load(fp)

    def _is_first_time_run(self, table_name):
        return not self.last_data_dir("DATAGEN", "SYN_TABLES", table_name.upper()) and not self.latest_delta_dir(
            "DELTA_TABLES", "SYN_TABLES", f"{table_name.upper()}.parquet")

    def _check_if_update_needed(self, path, tag):
        return len(self.glob(os.path.join(path, tag))) == 0

    def process_and_version_syn_extractions(self):

        for key, value in self.harmonization_config_dict['jobs'].items():
            if not value.get('enabled', False):
                print(f'Entry {key} disabled in config. Skipping..')
                continue

            if not value.get('is_standalone_table', False):
                continue

            tag = value.get('tag', None)
            table_id = value.get('id', None)
            syn_table_group_name = self.get_syn_table_group_name(table_id)
            assert (tag is not None and table_id is not None and syn_table_group_name is not None)

            latest_harmonized_dir, latest_orch_meta_dict = self.locate_harmonization_dir(syn_table_group_name)

            assert latest_harmonized_dir is not None
            assert (tag == latest_orch_meta_dict['tag'])
            print(f"Harmonization source: {syn_table_group_name}, tag: {tag}, input_dir: {latest_harmonized_dir},  ")
            # delta case where extractions are generated for delta data only. currently it is assumed to be full always
            delta_extractions = False
            files = self.dir_glob(os.path.join(latest_harmonized_dir, self.harmonized_out_dir))
            if files:
                self.SPARK_SESSION = self.init_spark_session()
                for file_path in files:
                    filename = file_path.split('/')[-2] if file_path.endswith('/') else file_path.split('/')[-1]
                    syn_config = self.SYSTEM_DICT["syn_tables"].get(filename.upper(), None)
                    if not syn_config:
                        continue
                    # currently returns true always
                    if not self._check_if_update_needed(file_path, tag):
                        print(f"Already latest tag is available. skipping..")
                        continue
                    elif self.glob(os.path.join(self.DATA_DIR, "DELTA_TABLES", "*", f"{syn_config['table_name']}*")) and self.skip_if_exists:
                        continue
                    else:
                        self.skip_if_exists = False
                    print(f'Generating orchestrator syn table for {file_path}')

                    df = self.read_parquet_to_df(file_path)
                    first_run = self._is_first_time_run(filename.upper())
                    if self.RUN_VERSION == syn_config.get("base_version"):
                        first_run = True

                    print(f"First time run for {filename}: {first_run}")

                    if not self.ees_full_update() and not first_run and delta_extractions:
                        raise NotImplementedError

                    self.write_versioned_syn_data(syn_config=syn_config, syn_data_df=df, is_base_version=first_run,
                                                  tag=tag)

    def run(self):
        if 'generate_sparqksql_syn_tables' == self.options.run:
            self.process_and_version_syn_extractions()


if __name__ == "__main__":
    HarmonizedSynTables().run()
