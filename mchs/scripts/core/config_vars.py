import os
import enum
import re

CORE_CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../config')
UTIL_CONFIG_FILE_NAME = os.path.join('config.json')
DEFAULT_DATABASE = 'MCHS'

PIPE_SEP = '|'
TAB_SEP = '\t'
DK_SUFFIX = 'dk'
DEFAULT_COUNTER = 1
INCR_COUNTER = "INCR_COUNTER"

DIR_DTM_FORMAT = "%Y.%m.%d_%H.%M.%S"
YEAR_FORMAT = '%Y-%m-%d'
MONTH_FORMAT = '%Y-%m'
MAYO_ENCODING = "ISO-8859-1"
LATEST_DELTA_DIR = "LATEST_DELTA_TABLES"

#date corresponding to 1900-01-01
DEFAULT_INVALID_DATE= -2209008070

NGRAM_SIZE = 3
PATIENT_PER_FILE = 10000
MAX_PROCESS = os.cpu_count()-1
ONE_GB = 1024 * 1024 * 1024
INT_MAX = 2147483647
FILE_PATTERN = re.compile(r'(?P<prefix>.*?)(?P<filetype>(FACT|DIM|SNF)_.*?)(?P<suffix>_((?P<start>\d+)_(?P<end>\d+)(_[^_]*)?|SmCohort|PHList|CathLab|DARA|HISTORICAL))?(_INC|_COVID)?\.txt.*')

DIAGNOSIS_METHOD_CODE_DICT = {
    'ICD10': 'ICD-10-CM',
    'ICD-10-CM Ext-CP': 'ICD-10-CM',
    'ICD9': 'ICD-9-CM',
    'I9': 'ICD-9-CM',
    'ICD-9-CM Ext-CP': 'ICD-9-CM',
    'HIC': 'HICDA',
    'SNOMED II': 'SNOMED CT',
    'SNOMED 2nd ed': 'SNOMED CT',
    'SNOMED International': 'SNOMED CT',
    'ICD-O-3 Range': 'ICD-O-3',
    'ICD-O-3 Topography': 'ICD-O-3',
    'MAYO: Problem Search Terminology': 'MAYO PST'
}

PROCEDURE_METHOD_CODE_DICT = {
    'ICD10': 'ICD-10-PCS',
    'ICD9': 'ICD-9-PCS',
    'CPT(R)': 'CPT'
}

EES_CURATOR_DICT = {
    #'EES_TABLE': ('SOURCE', 'category', 'name', 'dk_field', 'text_field', 'syn_config')
    #'FACT_DIAGNOSIS': ('FACT_DIAGNOSIS', 'disease', 'diagnosis', 'DIAGNOSIS_CODE_DK', 'DIAGNOSIS_DESCRIPTION', 'nfer_diagnosis_disease'),
    #'FACT_PROCEDURES': ('FACT_PROCEDURES', 'drug', 'procedure', 'PROCEDURE_CODE_DK', 'PROCEDURE_CODE', 'nfer_procedure_drug'),
    #'FACT_PROCEDURES_EXT': ('FACT_PROCEDURES', 'procedure', 'procedure', 'PROCEDURE_CODE_DK', 'PROCEDURE_DESCRIPTION', 'nfer_procedures_ext'),
    #'FACT_MEDS_ADMINISTERED': ('FACT_MEDS_ADMINISTERED', 'drug', 'medicine', 'MED_NAME_DK', 'ADMINISTERED_MEDS_COMMENTS', 'nfer_meds_administered_drug'),
    #'FACT_MED_INFUSION': ('FACT_MED_INFUSION', 'drug', 'infusion', 'MED_NAME_DK', '', 'nfer_med_infusion_drug'),
    #'FACT_IMMUNIZATIONS': ('FACT_IMMUNIZATIONS', 'drug', 'immune', 'VACCINE_NAME_DK', '', 'nfer_immunizations_drug'),
    #'FACT_VACCINE_MANUFACTURER': ('FACT_IMMUNIZATIONS', 'organization', 'immune', '', 'VACCINE_MANUFACTURER', 'nfer_vaccine_manufacturer'),
    #'FACT_ORDERS': ('FACT_ORDERS', 'drug', 'order', 'ORDER_ITEM_DK', 'MED_GENERIC', 'nfer_order_drug')
    #'FACT_ORDER_INSTRUCTIONS': ('FACT_ORDERS', 'drug', 'instructions', '', 'ORDER_INSTRUCTIONS', 'nfer_order_instructions')
}

DK_DICT = {
    'FACT_DIAGNOSIS'         : 'DIAGNOSIS_CODE_DK',
    'FACT_PROCEDURES'        : 'PROCEDURE_CODE_DK',
    'FACT_LAB_TEST'          : 'LAB_TEST_DK',
    'FACT_MEDS_ADMINISTERED' : 'MED_NAME_DK',
    'FACT_MED_INFUSION'      : 'INFUSION_MED_NAME_DK',
    'FACT_ORDERS'            : 'ORDER_ITEM_DK',
    'FACT_IMMUNIZATIONS'     : 'VACCINE_NAME_DK'
}

VACCINE_CODE_NAME_MAP_PREVIEW = {
    "1708": "pfizer",
    "1709": "moderna",
    "1710": "janssen",
    "1712": "unspecified",
    "1711": "astrazeneca"
}


VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION_PREVIEW = {
    "1708": 2,
    "1709": 2,
    "1710": 1,
    "1712": 2,
    "1711": 2
}

VACCINE_CODE_NAME_MAP_IRB = {
    "6067026"  : "pfizer",
    "7620026"  : "pfizer",
    "7890026"  : "pfizer",
    "6063026"  : "moderna",
    "6181026"  : "astrazeneca",
    "6309026"  : "janssen",
    "6789026"  : "sinopharm",
    "7209026"  : "sinopharm",
    "6838026"  : "novovax",
    "7026026"  : "sinovac",
    "7622026"  : "covaxin",
    "7619026"  : "epivaccorona",
    "7618027"  : "jiangsu",
    "7621027"  : "anhui",
    "7621026"  : "sputnik",
    "7620029"  : "sputnik",
    "7620027"  : "qazcovid",
    "7620028"  : "covivac",
    "7620030"  : "cansino",
    "6179026"  : "unspecified",
    "7618026"  : "unspecified",
    "8306026"  : "moderna",
    "8442028"  : "booster"
}

#cdap and irb -- are mapped to the same vaccines
VACCINE_CODE_NAME_MAP = VACCINE_CODE_NAME_MAP_IRB

VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION_IRB = {
    "6067026"  : 2,
    "7620026"  : 2,
    "7890026"  : 2,
    "6063026"  : 2,
    "6181026"  : 2,
    "6309026"  : 1,
    "6789026"  : 2,
    "7209026"  : 2,
    "6838026"  : 2,
    "7026026"  : 2,
    "7622026"  : 2,
    "7619026"  : 2,
    "7618027"  : 2,
    "7621027"  : 2,
    "7621026"  : 2,
    "7620029"  : 2,
    "7620027"  : 2,
    "7620028"  : 2,
    "7620030"  : 1,
    "6179026"  : 2,
    "7618026"  : 2,
    "8306026"  : 2,
    "8442028"  : 2
}

#cdap and irb -- are mapped to the same vaccines
VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION = VACCINE_REQUIRED_DOSE_FOR_FULL_VACCINATION_IRB

DAYS_FOR_FULL_VACCINATION = 14


class DataEnv(enum.Enum):
    MAYO_CDAP = 'MAYO_CDAP'
    MAYO_IRB = 'MAYO_IRB'
    MAYO_PREVIEW = 'MAYO_PREVIEW'


class RunMode(enum.Enum):
    Full = 'FULL'
    Delta = 'DELTA'


class RunStatus(enum.Enum):
    Success = "SUCCESS"
    Running = "RUNNING"
    Failed = "FAILED"