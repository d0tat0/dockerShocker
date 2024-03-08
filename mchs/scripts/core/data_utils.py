import csv
import datetime
import glob
import time
from collections import Counter
from optparse import OptionParser

from core.config_vars import *
from core.core_base import CoreBase


class UtilBase(CoreBase):
    def __init__(self, config_file = None):
        self.this_year = datetime.datetime.now().year
        self.this_year_epoch = int(time.time())
        self.file_pattern = FILE_PATTERN
        self.init_options()
        super().__init__(self.options.config if self.options.config else config_file)
        self.get_dirs()

    def set_options(self, parser):
        pass

    def common_options(self, parser):
        parser.add_option('-C', '--config', help="config file location")
        parser.add_option('-R', '--run', default='', help='command option to run')
        parser.add_option('', '--dry-run', action='store_true', default=False, help='dry run')
        parser.add_option('', '--debug', action='store_true', default=False, help='debug prints')
        parser.add_option('', '--test', action='store_true', default=False, help='testing mode')
        parser.add_option('', '--verbose', action='store_true', default=False, help='verbose prints')
        parser.add_option('', '--database', default=DEFAULT_DATABASE, help='database')
        parser.add_option('-S', '--source', default='', help='source')
        parser.add_option('-T', '--category', default='', help='category')
        parser.add_option('-V', '--version', default=None, help='Data Version')
        parser.add_option('-M', '--max-process', default=MAX_PROCESS, help='maximum process count')
        parser.add_option('-K', '--skip-if-exists', default='True', help='Skip Source Processing is Out Source Dir exists')
        parser.add_option('', '--columns', default=None, help='string of comma separated names of columns')
        parser.add_option('-H', '--cohort-config', help='Path to cohort config file')
        parser.add_option('', '--version-list', default=None, help='string of comma separated names of columns')
        parser.add_option('', '--mode', default=None, help='Similar to run')

    def init_options(self):
        parser = OptionParser()
        self.common_options(parser)
        self.set_options(parser)
        self.options, _ = parser.parse_args()

    def get_opt_list(self, opt):
        return opt.split(',') if opt else []

    def get_date_str(self, dtm):
        return time.strftime(YEAR_FORMAT, time.gmtime(dtm))

    def get_option_source_list(self, opt_source):
        source_list = []
        if opt_source.upper() == 'INFO:ALL':
            source_list.extend([ ('INFO', s.upper()) for s in self.SYSTEM_DICT['info_tables'].keys() ])
        elif opt_source.upper() == 'DATA:ALL':
            source_list.extend([ ('DATA', s.upper()) for s in self.SYSTEM_DICT['data_tables'].keys() ])
        elif opt_source.upper() == 'FACT:ALL':
            source_list.extend([ ('DATA', s.upper()) for s in self.SYSTEM_DICT['data_tables'].keys() if s.startswith('fact_') ])
        else:
            source_list = [ s.split(':') for s in self.get_opt_list(opt_source) ]
        return source_list

    def get_file_info(self, file_name):
        word_list = file_name.strip().split('_')
        if len(word_list) <= 4:
            return ''
        file_type = self.get_filetype(file_name)
        start_patient_id , end_patient_id = self.get_patient_range(file_name)
        return file_type, start_patient_id, end_patient_id

    def get_filetype(self, filename):
        basename = os.path.basename(filename)
        if "RHCCOHORT" in basename:
            basename = basename.replace("RHCCOHORT", "1_2")
            print("RHCCOHORT file found, changing filename to {}".format(basename))
        match_obj = self.file_pattern.match(basename)
        if not match_obj:
            return ''
        return match_obj.group('filetype')

    def get_patient_range(self, filename):
        start = end = 0
        basename = os.path.basename(filename)
        match_obj = self.file_pattern.match(basename)
        if not match_obj:
            return (start, end)
        if match_obj.group('start'):
            start = int(match_obj.group('start'))
        if match_obj.group('end'):
            end = int(match_obj.group('end'))
        return (start, end)

    def get_dim_file(self, name):
        table_dict = self.SYSTEM_DICT['info_tables'][name]
        file_name = table_dict['files'][0]
        file_list = glob.glob(os.path.join(self.DATAGEN_DIR, table_dict["dir_name"], f'{file_name}*'))
        print("file_list:", file_list, self.INPUT_SOURCE_DIR)
        return file_list[0] if file_list else ''

    def get_source_file_info_list(self, source):
        new_source_file_info_list = []
        for file_id in self.get_file_id_list(source):
            raw_file_name = self.get_raw_patient_file_name(source, file_id)
            source_file_name = os.path.join(self.INPUT_SOURCE_DIR, source, raw_file_name)
            file_type, start_patient_id, end_patient_id = self.get_file_info(source_file_name)
            new_source_file_info_list.append((file_type, start_patient_id, source_file_name))
        return sorted(new_source_file_info_list)

    def get_partition_block_id(self, worker_id):
        return worker_id * self.PATIENT_PARTITION_SIZE

    def get_partition_block(self, start_patient_id):
        return (start_patient_id // self.PATIENT_PARTITION_SIZE) * self.PATIENT_PARTITION_SIZE

    def get_process_block_id(self, worker_id):
        return worker_id * self.PROCESS_PARTITION_SIZE

    def get_process_block(self, start_patient_id):
        return (start_patient_id // self.PROCESS_PARTITION_SIZE) * self.PROCESS_PARTITION_SIZE

    def iterate_partition_block(self, worker_id):
        process_block_id = self.get_process_block_id(worker_id)
        start_block_id = process_block_id * self.MAX_BLOCKS
        end_block_id = (process_block_id + self.PROCESS_PARTITION_SIZE) * self.MAX_BLOCKS
        while start_block_id < end_block_id:
            next_block_id = start_block_id + (self.PATIENT_PARTITION_SIZE * self.MAX_BLOCKS)
            yield (start_block_id, next_block_id)
            start_block_id = next_block_id

    def file_id_list(self, range_obj, big_table):
        file_ids = []
        for i in range_obj:
            if big_table:
                for j in range(5):
                    file_ids.append(i*PATIENT_PER_FILE + j*PATIENT_PER_FILE//5 + 1)
            else:
                file_ids.append(i*PATIENT_PER_FILE + 1)
        return file_ids

    def get_file_id_list(self, source):
        big_table = source in self.BIG_TABLES
        range_obj = range(self.MAX_BUCKETS)
        return self.file_id_list(range_obj, big_table)

    def get_target_source_dir(self, target, source, version=None):
        if not version:
            return os.path.join(self.DATA_DIR, target, source)
        return os.path.join(self.ROOT_DIR, version, target, source)

    def get_source_dir(self, source, version=None):
        return self.get_target_source_dir('DATAGEN', source, version)

    def get_dim_file_name(self, source, name):
        source_dir = self.get_source_dir(source)
        return os.path.join(source_dir, '%s.final' % name.lower())

    def get_raw_patient_file_name(self, source, start_patient_id):
        patient_per_file = PATIENT_PER_FILE//5 if source in self.BIG_TABLES else PATIENT_PER_FILE
        end_patient_id = start_patient_id - 1 + patient_per_file
        return f'0_0_{source}_{start_patient_id}_{end_patient_id}.txt'

    def get_patient_final_file_name(self, start_patient_id):
        partition_block = self.get_partition_block(start_patient_id)
        file_name = 'patient_%07d_%07d.final' % (partition_block, start_patient_id)
        return file_name

    def get_patient_dk_file_name(self, source, start_patient_id):
        source_dir = self.get_source_dir(source)
        partition_block = self.get_partition_block(start_patient_id)
        file_name = 'patient_%07d_%07d.%s' % (partition_block, start_patient_id, DK_SUFFIX)
        return os.path.join(source_dir, file_name)

    def get_patient_suffix_file_name(self, file_name, suffix):
        return file_name.replace('.%s' % DK_SUFFIX, '.%s' % suffix)

    def get_norm_patient_id(self, start_patient_id):
        d, m = divmod(start_patient_id, PATIENT_PER_FILE)
        d *= PATIENT_PER_FILE
        return d, m

    def get_base_patient_id(self, start_patient_id):
        # patient id overlaps
        base_patient_id, _ = self.get_norm_patient_id(start_patient_id)
        base_patient_id = (base_patient_id * self.MAX_BLOCKS) + 1
        return base_patient_id

    def get_patient_id_file_name(self, start_patient_id):
        d, m = self.get_norm_patient_id(start_patient_id)
        norm_patient_id = d + 1
        patient_id_file_name = self.get_patient_dk_file_name(self.DIM_PATIENT, norm_patient_id)
        return self.get_patient_suffix_file_name(patient_id_file_name, 'dkid')

    def get_patient_file_block_numbers(self, source, file_name):
        n, b, s = file_name.split('_')
        s = s.split('.')[0]
        return (n, b, s)

    def get_patient_file_norm_block_numbers(self, source, file_name):
        n, b, s = self.get_patient_file_block_numbers(source, file_name)
        s = '%07d' % (((int(s) // PATIENT_PER_FILE) * PATIENT_PER_FILE) + 1, )
        return (n, b, s)

    def get_patient_file_name_from_block(self, source, b, s):
        return 'patient_%s.final' % '_'.join([b, s])

    def get_patient_file_format(self, source, process_id, suffix):
        source_dir = self.get_source_dir(source)
        file_format = 'patient_%d*.%s' % (process_id, suffix)
        file_format = os.path.join(source_dir, file_format)
        return file_format

    def get_patient_file_list(self, source, version=None):
        new_source = self.DIM_PATIENT if source == 'DIM_PATIENT_DK' else source
        source_dir = self.get_source_dir(new_source, version)
        suffix = 'dkid' if source == 'DIM_PATIENT_DK' else 'final'
        return sorted(glob.glob(os.path.join(source_dir, 'patient_*.%s' % suffix)))

    def get_packaged_file_name(self, source_file_name):
        return source_file_name.replace(self.RUN_VERSION, self.NFERX_VERSION)

    def get_patient_dim_from_source(self, source, dim_source_dir, base_file_name):
        dim_file_name = os.path.join(dim_source_dir, base_file_name)
        b_n, b_b, b_s = self.get_patient_file_norm_block_numbers(source, base_file_name)
        dim_file_name = self.get_patient_file_name_from_block(source, b_b, b_s)
        return b_b, dim_file_name

    def generate_patient_buckets(self, source):
        source_type, source = source.split(':') if ':' in source else ('DATA', source)
        source_dir = self.get_source_dir(source)
        file_list = glob.glob(os.path.join(source_dir, 'patient_*.final'))
        print('PATIENT BUCKET:', source, source_dir, len(file_list))
        bucket_dict = {}
        for i in range(self.options.max_process):
            bucket_dict[i] = {}
        process = self.options.max_process - 1
        for file_name in sorted(file_list):
            base_file_name = os.path.basename(file_name)
            b_n, b_b, b_s = self.get_patient_file_norm_block_numbers(source, base_file_name)
            dim_file_name = self.get_patient_file_name_from_block(source, b_b, b_s)
            if b_b not in bucket_dict[process]:
                process = (process + 1) % self.options.max_process
            bucket_dict[process].setdefault(b_b, []).append([base_file_name, dim_file_name])
        return bucket_dict

    def get_process_bucket_file_list(self, bucket_dict, process):
        file_list = []
        for block_id, block_file_list in bucket_dict[process].items():
            file_list.extend(block_file_list)
        return sorted(file_list)

    def get_ngrams(self, phrase, ngram_size=NGRAM_SIZE):
        word_list = phrase.split()
        ngram_list = set()
        for i in range(len(word_list)):
            for j in range(ngram_size):
               ngram_list.add(' '.join(word_list[i:i+j+1]))
        return ngram_list

    def get_norm_patient_type(self, patient_type):
        patient_type = patient_type.lower()
        if 'inpatient' in patient_type or patient_type in ('ip transitional care', 'ip behavioral health', 'ip rehab', 'ip hospice'):
            patient_type = 'inpatient'
        elif 'emergency' in patient_type:
            patient_type = 'emergency'
        elif 'op in bed' in patient_type or 'op bed' in patient_type or 'overnight' in patient_type:
            patient_type = 'overnight bed'
        else:
            patient_type = 'outpatient'
        return patient_type

    def get_dk_code_info(self, file_name):
        code_dir = self.get_target_source_dir('CODES', 'CODES_MAPPING', version="NFERX")
        code_file_name = os.path.join(code_dir, file_name)
        return self.get_json_data(code_file_name)

    def get_surgical_case_procedure_code_info(self):
        return self.get_dk_code_info('surgical_case_dim_surgical_procedure.json', True)

    def get_diagnosis_dim_diagnosis_code_info(self):
        return self.get_dk_code_info('diagnosis_dim_diagnosis_code.json', True)

    def get_procedures_dim_procedure_code_info(self):
        return self.get_dk_code_info('procedures_dim_procedure_code.json', True)

    def get_diagnosis_code_info(self):
        return self.get_dk_code_info('fact_diagnosis_dk_code.json')

    def get_procedure_code_info(self):
        return self.get_dk_code_info('fact_procedures_dk_code.json')

    def get_lab_test_loinc_info(self):
        return self.get_dk_code_info('fact_lab_test_dk_code.json')

    def get_medicine_info(self):
        return self.get_dk_code_info('fact_meds_administered_dk_code.json')

    def get_order_info(self):
        return self.get_dk_code_info('fact_orders_dk_code.json')

    def get_echo_info(self):
        return self.get_json_data('echo_test_dk_codes.json')

    def get_disease_dict(self, disease_csv_file_name):
        disease_dict = {}
        with open(disease_csv_file_name) as csv_file:
            for row_dict in csv.DictReader(csv_file):
                disease_id, disease = [ row_dict.get(n) for n in ('ID', 'Disease') ]
                disease_id = int(disease_id)
                method_code, diag_code = [ row_dict.get(n) for n in ('ICD Method', 'ICD Code') ]
                _, dk_list = disease_dict.setdefault(disease_id, ( '', set() ))
                dk_list.add((method_code, diag_code))
                disease_dict[disease_id] = (disease, dk_list)
        return disease_dict

    def get_consolidated_stats(self, file_format):
        counter = Counter()
        for file_name in glob.glob(file_format):
            json_dict = self.get_json_data(file_name)
            print(file_name, len(json_dict))
            counter.update(json_dict)
            os.remove(file_name)
        return counter

    def create_dir_with_headers(self, dir_path, nfer_schema, type_schema):
        os.makedirs(dir_path, exist_ok=True)
        header_file_name = os.path.join(dir_path, 'header.csv')
        if type_schema in ["INTEGER", "STRING"]:
            type_schema = [type_schema for _ in nfer_schema]
        with open(header_file_name, 'w') as f:
            f.write('%s\n' % self.csv_separator.join(nfer_schema))
            f.write('%s\n' % self.csv_separator.join(type_schema))

    def trigger_dag(self, dag_id):
        dag_command = f'airflow dags trigger {dag_id}'
        self.run_cmd(dag_command, log=True)

    def distribute_splice_files(self, max_bins, info_list):
        splice_dict = {}
        bin_dict = dict([(i, []) for i in range(max_bins)])
        i = 0
        for info in info_list:
            file_type, start_patient_id, source_file_name = info
            old_bin = splice_dict.get(start_patient_id, -1)
            if old_bin >= 0:
                new_bin = old_bin
            else:
                new_bin = i
                splice_dict[start_patient_id] = new_bin
                i = (i + 1) % max_bins
            bin_dict[new_bin].append(info)
        return bin_dict

    def get_latest_folder(self,base_dirs):
        latest_folder = None
        latest_time = datetime.datetime.min

        print(f"Locating latest harmonized interim..")
        for base_dir in base_dirs:
            for folder_path in self.dir_glob(base_dir):
                folder_name = folder_path.split('/')[-2] if folder_path.endswith('/') else folder_path.split('/')[-1]
                print(f'\tlooking in {folder_path}')

                try:
                    folder_time = datetime.datetime.strptime(folder_name, '%Y.%m.%d_%H.%M.%S')
                    if folder_time > latest_time:
                        latest_time = folder_time
                        latest_folder = folder_path
                except ValueError:
                    continue

        return latest_folder

    # Given a syn table or extraction table id, returns the name of the table
    def get_syn_table_group_name(self, table_id):

        for table_group_name_key, value in self.SYSTEM_DICT['extraction_jobs']['tables'].items():
            if value['id'] == table_id:
                return table_group_name_key
        return None
