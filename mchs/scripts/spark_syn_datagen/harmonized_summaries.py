import os
import time
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, DoubleType

from spark_jobs.orchestrator import Orchestrator
from pyspark.sql.window import Window
from pyspark.sql.functions import col, round
from core.config_vars import RunMode
from spark_misc import dfutils


ONE_GB = 1024 * 1024 * 1024
VARIABLE_TSV_FILE = 'variable.tsv'
LAB_TEST_TSV_FILE = 'LabTestEntity.tsv'
FLOWSHEET_TSV_FILE = 'FlowSheetEntity.tsv'
CATEGORICAL_TSV_FILE = 'CategoricalEntityFile.tsv'
COMBINED_TABLE_NAME = 'FACT_HARMONIZED_MEASUREMENTS'
SOURCE_TABLE_LABTEST = 'FACT_LAB_TESTS'
SOURCE_TABLE_FLOWSHEET = 'FACT_FLOWSHEETS'

CATEGORTICAL_COLNAME = "TYPE"
CATEGORTICAL_COLNAME_CATEGORY = "CATEGORICAL"
CATEGORTICAL_COLNAME_NUMERIC = "NUMERIC"

CUSTOM_RANGES_DICT = {
    'FACT_FLOWSHEETS':
    {
        "PROMIS-10 Global Mental Health T-Score":{"min": 0 , "max": 100},
        "PROMIS-10 Global Physical Health T-Score":{"min": 0 , "max": 100},
        "Vent positive end expiratory pressure":{"min": 0 , "max": 40},
        "Eastern Cooperative Oncology Group (ECOG) Performance Status Scale":{"min": 0 , "max": 5},
        "Mean arterial pressure":{"min": 0 , "max": 200},
        "Modified Rankin Scale (MRS)":{"min": 0 , "max": 6},
    },
    'FACT_LAB_TESTS':
    {
        '24 hour, urine , glucose':{"min": 0 , "max": 2400},
         'Antinuclear Antibodies (ANA)':{"min": 0 , "max": 2048},
         'Beta-2-Microglobulin, CSF':{"min": 0 , "max": 100 },
         'Beta-2-Microglobulin, Urine':{"min": 0 , "max": 50000 },
         'Carboxyhemoglobin fractional in Venous blood':{"min": 0 , "max": 100},
         'Ceruloplasmin in Serum':{"min": 0 , "max": 100},
         'Estimated Glomerular Filtration Rate (eGFR) -- Unspecified':{"min": 0 , "max": 300},
         'Hemoglobin in Stool (Quantitative)':{"min": 0 , "max": 100},
         'IgG Clearance / Albumin Clearance Ratio in Serum and CSF':{"min": 0 , "max": 5},
         'IgG/Albumin Ratio in CSF':{"min": 0 , "max": 2},
         'Myelin Basic Protein in CSF':{"min": 0 , "max": 100},
         'Oligoclonal Banding in Serum':{"min": 0 , "max": 10},
         'Oxalate, Serum':{"min": 0 , "max": 150},
         'Oxalate:Creatinine Ratio, Urine':{"min": 0 , "max": 1.5},
         'Oxygen content arterial blood':{"min": 0 , "max": 100},
         'Oxygen content in blood':{"min": 0 , "max": 100},
         'Oxygen content venous blood':{"min": 0 , "max": 100},
         'carboxyhaemoglobin':{"min": 0 , "max": 100},
         'carboxyhaemoglobin (fractional)':{"min": 0 , "max": 100},
         'hemoglobin A2':{"min": 0 , "max": 100},
         'hemoglobin F':{"min": 0 , "max": 100},
         'ph serum/plasma':{"min": 6 , "max": 8},
         'unconjugated bilirubin(indirect bilirubin) in serum/plasma':{"min": 0 , "max": 100},
         'urobilinogen, urine':{"min": 0 , "max": 10}
     }
 }

def dict_to_sparkdf(d,spark):
    data = []
    schema = StructType([
        StructField('nfer_varname', StringType(), False),
        StructField('range_min', DoubleType(), False),
        StructField('range_max', DoubleType(), False)
    ])

    for key, val in d.items():
        data.append((key, float(val['min']), float(val['max'])))
    sdf = spark.createDataFrame(data=data, schema=schema)
    sdf.printSchema()

    return sdf

class HarmonizedSummaries(Orchestrator):

    def __init__(self, debug=False, demo=False):
        super().__init__()
        self.FLOWSHEET_COL_MAP = None
        self.LABTEST_COL_MAP = None
        self.parent = self
        self.debug = debug
        self.demo = demo
        self.RUN_MODE = RunMode.Full.value



    def get_mapping_table(self, syn_config, spark,
                          file_name=LAB_TEST_TSV_FILE,  # LabTestEntity.tsv
                          columns=[],
                          return_all_columns=False,
                          mode='numeric'):  # mode is numeric or categorical

        base_path = syn_config['rule_file_location']
        print(f'Reading mappings from {base_path}')

        variable_file_path = os.path.join(base_path, VARIABLE_TSV_FILE)
        variable_df = self.read_csv_to_df(variable_file_path, sep='$$$')

        # selecting only valid variables
        if mode == 'numeric':
            variable_df = variable_df.filter('DerivedType == 99 and VariableState == 4')
        else:
            variable_df = variable_df.filter('DerivedType == 4 and VariableState == 4')

        if not return_all_columns:
            variable_df = variable_df.select(['VariableId', 'DisplayName', 'VariableName', 'Units','IsValidRangeBool','RangeKnobMax','RangeKnobMin'])

        # duplicate name issue
        variable_df = variable_df.withColumnRenamed('Units', 'HARMONIZED_UNITS')
        variable_df = variable_df.withColumnRenamed('Description', 'VariableTSVDescription')

        # joining with entity table
        df = self.read_csv_to_df(
            os.path.join(base_path, file_name),
            sep="$$$"
        )
        df = df.join(variable_df, 'VariableId')
        # if displayname is none pick variablename
        df = df.withColumn("DisplayName",
                           F.when(df.DisplayName.isNull(), df.VariableName).otherwise(df.DisplayName)
                           )
        df = df.filter('Excluded==false')

        if columns:
            df = df.select(columns)

        print(f'Total no of rules to join on: {df.count()}')

        return df

    def generate_harmonized_labtest_summary_hyc(self, version=None,
                                                return_df=False,
                                                latest_only=False
                                                ):
        print("------------------  Starting Lab Test Processing  -----------------------------")
        # READ DATA
        start = time.perf_counter()
        spark: SparkSession = self.init_spark_session()
        syn_config = self.SYSTEM_DICT["syn_tables"]["harmonized_labtests"]
        read_dir_path = syn_config['source_dir']  # absolute path
        nfer_schema = syn_config['nfer_schema']
        if return_df:
            labtest_entity_tsv_df = self.get_mapping_table(syn_config, spark, return_all_columns=True)
        else:
            labtest_entity_tsv_df = self.get_mapping_table(syn_config, spark)

        debug_mode = self.debug
        demo_mode = self.demo

        # excluded "LAB_RESULT_DTM",
        labtest_cols = ["NFER_PID", "NFER_DTM", "LAB_TEST_DK", "NORMAL_RANGE_TXT", "RESULT_TXT"]

        columns_from_dim_labtest = ['STANDARD_LAB_TEST_CODE', 'STANDARD_LAB_TEST_DESCRIPTION',
                                    'LAB_TEST_DESCRIPTION']
        columns_from_fact_labtest = ['LAB_TEST_DK',
                                     'UNIT_OF_MEASURE_TXT', 'NORMAL_RANGE_TXT']

        fact_labtest_join_cols = ['LAB_TEST_DK', 'STANDARD_LAB_TEST_CODE', 'STANDARD_LAB_TEST_DESCRIPTION',
                                  'UNIT_OF_MEASURE_TXT', 'NORMAL_RANGE_TXT', 'LAB_TEST_DESCRIPTION']
        labtest_entity_join_cols = ['DK', 'StandardCode', 'Description',
                                    'Units', 'NormalRange', 'LabTestDescription']
        if demo_mode:
            FACT_FLOWSHEET_PATH = '/data3/ORCHESTRATOR/4.001/DATAGEN/FACT_LAB_TEST/'
            FILENAME = 'patient_0000000_0000001.final'
            df = dfutils.read_one_file(self, spark, FACT_FLOWSHEET_PATH, FILENAME)
        else:
            df = self.read_versioned_datagen_dir(read_dir_path,version=version,latest_only=latest_only)

        if not return_df:
            df = df.select(list(set(labtest_cols + columns_from_fact_labtest + ['ROW_ID', 'FILE_ID', 'NFER_AGE'])))

        # join with dim table to get extra needed columns
        dim_select_cols = ['LAB_TEST_DK', 'STANDARD_LAB_TEST_CODE', 'STANDARD_LAB_TEST_DESCRIPTION',
                           'LAB_TEST_DESCRIPTION']
        labtest_dim_file_alias = "dim_lab_test_code"
        dim_df = self.read_versioned_dim_table(labtest_dim_file_alias).select(dim_select_cols)
        dim_df = dim_df.withColumnRenamed('LAB_TEST_DK', 'LAB_TEST_DK_DIM')
        df = df.join(F.broadcast(dim_df), (df.LAB_TEST_DK == dim_df.LAB_TEST_DK_DIM))

        # normalize the file columns to match with the mapping file contents
        for col in fact_labtest_join_cols:
            df = dfutils.normalize_trim_col(df, col)

        for col in labtest_entity_join_cols:
            labtest_entity_tsv_df = dfutils.normalize_trim_col(labtest_entity_tsv_df, col)

        df.cache()
        if debug_mode:
            print(f'Number of labtest rows pre joining with harmonization table: {df.count()}')
        df_numeric = df.join(F.broadcast(labtest_entity_tsv_df),
                             (df[fact_labtest_join_cols[0]] == labtest_entity_tsv_df[labtest_entity_join_cols[0]]) &
                             (df[fact_labtest_join_cols[1]].eqNullSafe(
                                 labtest_entity_tsv_df[labtest_entity_join_cols[1]])) &
                             (df[fact_labtest_join_cols[2]].eqNullSafe(
                                 labtest_entity_tsv_df[labtest_entity_join_cols[2]])) &
                             (df[fact_labtest_join_cols[3]].eqNullSafe(
                                 labtest_entity_tsv_df[labtest_entity_join_cols[3]])) &
                             (df[fact_labtest_join_cols[4]].eqNullSafe(
                                 labtest_entity_tsv_df[labtest_entity_join_cols[4]]))
                             )
        df_numeric.cache()

        labtest_entity_tsv_df_cat = self.get_mapping_table(syn_config, spark,
                                                           file_name=LAB_TEST_TSV_FILE,
                                                           return_all_columns=True,
                                                           mode='category')

        categorical_tsv_df = self.read_csv_to_df(
            os.path.join(syn_config['rule_file_location'], CATEGORICAL_TSV_FILE),
            sep="$$$"
        )
        categorical_tsv_df = categorical_tsv_df.withColumnRenamed('Description',
                                                                  'CategoricalTSVDescription')
        categorical_tsv_df = categorical_tsv_df.withColumnRenamed('Patients',
                                                                  'CategoricalTSVPatients')
        categorical_tsv_df = categorical_tsv_df.withColumnRenamed('VariableID',
                                                                  'CategoricalTSVVariableID')
        categorical_tsv_df = categorical_tsv_df.withColumnRenamed('CreatedAt',
                                                                  'CategoricalTSVCreatedAt')

        labtest_entity_tsv_df_cat = labtest_entity_tsv_df_cat.join(F.broadcast(categorical_tsv_df),
                                                                   (labtest_entity_tsv_df_cat["EntityID"] ==
                                                                    categorical_tsv_df["EntityId"])).drop(
            labtest_entity_tsv_df_cat.EntityID)
        for col in labtest_entity_join_cols:
            labtest_entity_tsv_df_cat = dfutils.normalize_trim_col(labtest_entity_tsv_df_cat, col)

        df_cat = df.join(F.broadcast(labtest_entity_tsv_df_cat),
                         (df[fact_labtest_join_cols[0]] == labtest_entity_tsv_df_cat[labtest_entity_join_cols[0]]) &
                         (df[fact_labtest_join_cols[1]].eqNullSafe(
                             labtest_entity_tsv_df_cat[labtest_entity_join_cols[1]])) &
                         (df[fact_labtest_join_cols[2]].eqNullSafe(
                             labtest_entity_tsv_df_cat[labtest_entity_join_cols[2]])) &
                         (df[fact_labtest_join_cols[3]].eqNullSafe(
                             labtest_entity_tsv_df_cat[labtest_entity_join_cols[3]])) &
                         (df[fact_labtest_join_cols[4]].eqNullSafe(
                             labtest_entity_tsv_df_cat[labtest_entity_join_cols[4]])) &
                         (df['RESULT_TXT'] == labtest_entity_tsv_df_cat['OriginalToken'])
                         )
        df_cat.cache()

        if debug_mode:
            print(f'Number of labtest rows post joining with harmonization table for numeric: {df_numeric.count()}')
            print(f'Number of labtest rows post joining with harmonization table for cat: {df_cat.count()}')
        # rename or delete columns added from mapping table as required
        if not return_df:
            df_numeric = df_numeric.withColumnRenamed('DisplayName', 'DISPLAY_NAME')
        df_numeric = df_numeric.drop(F.col('DK'))
        df_cat = df_cat.drop(F.col('DK'))

        # drop null dk and result column, NFER_PID also
        df_numeric = df_numeric.na.drop(subset=["NFER_PID", "LAB_TEST_DK"])
        df_cat = df_cat.na.drop(subset=["NFER_PID", "LAB_TEST_DK"])

        # strip and lowecase
        df_numeric = dfutils.normalize_trim_col(df_numeric, 'RESULT_TXT')
        df_numeric = df_numeric.withColumn("RESULT_TXT", round(df_numeric.RESULT_TXT.cast(DoubleType()), 2))
        df_numeric = df_numeric.na.drop(subset=["RESULT_TXT"])  # non double numbers will become: null
        if debug_mode:
            print(f'fact labtest size post dropping null or non parsable records: {df_numeric.count()}')

        """
        |       Formula|
        +--------------+
        |   _value/1000|
        |        _value|
        |        _Value|
        |     _value*10|
        | _value / 1000|
        |   _value*4.01|
        |     _value/10|
        | _value/ 0.105|
        |  _value * 100|
        |    _value * 2|
        | _value * 1000|
        |_value / 0.105|
        |    _value/100|
        |  _value/0.105|
        |    _value /10|
        |  _value*88.02|
        |  _value*0.001|
        """
        df_numeric = df_numeric.withColumn("RESULT",
                                           F.when(F.col("Formula") == F.lit("_value/1000"), F.col("RESULT_TXT") / 1000)
                                           .when(F.col("Formula") == F.lit("_value*10"), F.col("RESULT_TXT") * 10)
                                           .when(F.col("Formula") == F.lit("_value / 1000"), F.col("RESULT_TXT") / 1000)
                                           .when(F.col("Formula") == F.lit("_value*4.01"), F.col("RESULT_TXT") * 4.01)
                                           .when(F.col("Formula") == F.lit("_value/10"), F.col("RESULT_TXT") / 10)
                                           .when(F.col("Formula") == F.lit("_value/ 0.105"),
                                                 F.col("RESULT_TXT") / 0.105)
                                           .when(F.col("Formula") == F.lit("_value * 100"), F.col("RESULT_TXT") * 100)
                                           .when(F.col("Formula") == F.lit("_value * 2"), F.col("RESULT_TXT") * 2)
                                           .when(F.col("Formula") == F.lit("_value * 1000"), F.col("RESULT_TXT") * 1000)
                                           .when(F.col("Formula") == F.lit("_value / 0.105"),
                                                 F.col("RESULT_TXT") * 0.105)
                                           .when(F.col("Formula") == F.lit("_value/100"), F.col("RESULT_TXT") / 100)
                                           .when(F.col("Formula") == F.lit("_value/0.105"), F.col("RESULT_TXT") / 0.105)
                                           .when(F.col("Formula") == F.lit("_value /10"), F.col("RESULT_TXT") / 10)
                                           .when(F.col("Formula") == F.lit("_value*88.02"), F.col("RESULT_TXT") * 88.02)
                                           .when(F.col("Formula") == F.lit("_value*0.001"), F.col("RESULT_TXT") * 0.001)
                                           .when(F.col("Formula") == F.lit("_value*65.10416"), F.col("RESULT_TXT") * 65.10416)
                                           .otherwise(F.col("RESULT_TXT"))
                                           )

        # filter values within range rangeKnobMax and min
        if syn_config['remove_outliers']:
            df_numeric = df_numeric.filter((
                                        (F.col('IsValidRangeBool') == 'true') &
                                       (F.col("RESULT") <= F.col("RangeKnobMax")) &
                                       (F.col("RESULT") >= F.col("RangeKnobMin"))) |
                                        (F.col('IsValidRangeBool') == 'false')
                                        )
        # add custom ranges
        df_numeric_join_col = 'DISPLAY_NAME' if 'DISPLAY_NAME' in df_numeric.columns else 'DisplayName'
        fdf = dict_to_sparkdf(CUSTOM_RANGES_DICT['FACT_LAB_TESTS'],spark)
        df_numeric = df_numeric.join(F.broadcast(fdf),df_numeric[df_numeric_join_col] == fdf['nfer_varname'],how='left',
                                     )
        df_numeric = df_numeric.filter(
                                            (F.col('nfer_varname').isNull()) |
                                           (
                                                (F.col('nfer_varname').isNotNull()) &
                                                (F.col("RESULT") <= F.col("range_max")) &
                                                (F.col("RESULT") >= F.col("range_min"))
                                           )
                                        )
        if debug_mode:
            print(f'Final Data size for numeric labtest: {df_numeric.count()}')
            if df_cat is not None:
                print(f'Final Data size for categorical labtest: {df_cat.count()}')
        df_cat = df_cat.withColumnRenamed("GroupName", "RESULT")

        if return_df:
            return df_numeric, df_cat

        # WRITE DATA
        source = syn_config["dir_name"].split("/")[-1]
        self.write_final_dir("SYN_DATA", df_numeric, nfer_schema, source)
        end = time.perf_counter()
        print("HARMONIZED_LABTEST : Total Time =", end - start)

    def generate_harmonized_flowsheet_summary_hyc(self,
                                                  version=None,
                                                  return_df=False,
                                                  is_categorical_flowsheet_present=False,
                                                  latest_only=False
                                                  ):
        print("------------------  Starting FLOWSHEET Processing  -----------------------------")
        # READ DATA
        start = time.perf_counter()
        spark: SparkSession = self.init_spark_session()
        syn_config = self.SYSTEM_DICT["syn_tables"]["harmonized_flowsheets"]
        read_dir_path = syn_config['source_dir']  # absolute path
        write_dir_name = syn_config['dir_name']
        nfer_schema = syn_config['nfer_schema']

        demo_mode = self.demo
        debug_mode = self.debug

        flowsheet_entity_tsv_df = self.get_mapping_table(syn_config, spark, FLOWSHEET_TSV_FILE,
                                                         return_all_columns=True)
        flowsheet_entity_select_cols = ['Formula', 'RowNameDK', 'DisplayName', 'HARMONIZED_UNITS',
                                        'Units', 'RowDescription', 'TypeDescription', 'SubTypeDescription',
                                        'IsValidRangeBool','RangeKnobMax','RangeKnobMin']
        if not return_df:
            flowsheet_entity_tsv_df = flowsheet_entity_tsv_df.select(flowsheet_entity_select_cols)

        if is_categorical_flowsheet_present:
            flowsheet_entity_tsv_df_cat = self.get_mapping_table(syn_config, spark,
                                                                 file_name=FLOWSHEET_TSV_FILE,
                                                                 return_all_columns=True,
                                                                 mode='category')
            print('Size from variable tsv: ', flowsheet_entity_tsv_df_cat.count())
            categorical_tsv_df = self.read_csv_to_df(
                os.path.join(syn_config['rule_file_location'], CATEGORICAL_TSV_FILE),
                sep="$$$"
            )
            print('Size categorical tsv df: ', categorical_tsv_df.count())
            categorical_tsv_df = categorical_tsv_df.withColumnRenamed('Description',
                                                                      'CategoricalTSVDescription')
            flowsheet_entity_tsv_df_cat = flowsheet_entity_tsv_df_cat.join(F.broadcast(categorical_tsv_df),
                                                                           (flowsheet_entity_tsv_df_cat["EntityID"] ==
                                                                            categorical_tsv_df[
                                                                                "EntityId"])).drop(
                categorical_tsv_df.EntityId)

        if debug_mode:
            print(f'Total no of numeric rules to join flowsheet on: {flowsheet_entity_tsv_df.count()}')
            # print(f'Total no of categorical rules to join flowsheet on: {flowsheet_entity_tsv_df_cat.count()}')

        # read flowsheet and process
        flowsheet_cols = [
            "NFER_PID",
            "NFER_DTM",
            "NFER_AGE",
            'FLOWSHEET_ROW_NAME_DK',
            "FLOWSHEET_RESULT_TXT",
            "ROW_ID",
            "FILE_ID",
            'FLOWSHEET_UNIT_OF_MEASURE_TXT',
            'FLOWSHEET_TYPE_DESCRIPTION',
            'FLOWSHEET_SUBTYPE_DESCRIPTION'
        ]
        dks_to_filter_list = flowsheet_entity_tsv_df.select("RowNameDK").rdd.flatMap(lambda
                                                                                    x: x).collect()  # + flowsheet_entity_tsv_df_cat.select("RowNameDK").rdd.flatMap(lambda x: x).collect()
        print(f'Total no of categorical+numeric dks to join flowsheet on: {len(dks_to_filter_list)}')

        if not self.demo:
            df = self.read_versioned_datagen_dir(read_dir_path,
                                                 version=version,
                                                 filters={'FLOWSHEET_ROW_NAME_DK': dks_to_filter_list},
                                                 latest_only = latest_only
                                                 )
        else:
            FACT_FLOWSHEET_PATH = '/data3/ORCHESTRATOR/4.001/DATAGEN/FACT_FLOWSHEETS/'
            FILENAME = 'patient_0000000_0000001.final'
            df = dfutils.read_one_file(self, spark, FACT_FLOWSHEET_PATH, FILENAME)
            df = self.assign_file_id_for_nfer_pid(df,"FACT_FLOWSHEETS")
        if not return_df:
            df = df.select(flowsheet_cols)

        df.cache()

        # drop null dk and result column, NFER_PID also
        df = df.na.drop(subset=["NFER_PID", "FLOWSHEET_ROW_NAME_DK"])

        df_flowsheet_dim = self.read_versioned_dim_table("dim_flowsheet_row_name")
        df_flowsheet_dim = df_flowsheet_dim.select(['FLOWSHEET_ROW_DESCRIPTION', 'FLOWSHEET_ROW_NAME_DK'])
        df_flowsheet_dim = df_flowsheet_dim.dropDuplicates(
            ['FLOWSHEET_ROW_NAME_DK'])  # one dk -> multiple row description
        df_flowsheet_dim = df_flowsheet_dim.withColumnRenamed('FLOWSHEET_ROW_NAME_DK', 'DIM_FLOWSHEET_ROW_NAME_DK')
        df = df.join(F.broadcast(df_flowsheet_dim),
                     df_flowsheet_dim.DIM_FLOWSHEET_ROW_NAME_DK == df.FLOWSHEET_ROW_NAME_DK)
        if self.debug:
            print(f'Number post join with DIM_FLOWSHEET_ROW_NAME for adding FLOWSHEET_ROW_DESCRIPTION col: {df.count()}')

        # strip and lowercase both entity and fact tables
        flowsheet_tsv_join_cols = ['RowDescription', 'RowNameDK', 'Units',
                                   'TypeDescription', 'SubTypeDescription']
        fact_table_join_columns = ['FLOWSHEET_ROW_DESCRIPTION', 'FLOWSHEET_ROW_NAME_DK',
                                   'FLOWSHEET_UNIT_OF_MEASURE_TXT',
                                   'FLOWSHEET_TYPE_DESCRIPTION', 'FLOWSHEET_SUBTYPE_DESCRIPTION']

        for col in fact_table_join_columns:
            df = dfutils.normalize_trim_col(df, col)
        for col in flowsheet_tsv_join_cols:
            flowsheet_entity_tsv_df = dfutils.normalize_trim_col(flowsheet_entity_tsv_df, col)

        # normalize the formula column
        flowsheet_entity_tsv_df = dfutils.normalize_trim_col(flowsheet_entity_tsv_df, 'Formula')
        df.cache()
        df_numeric = df.join(F.broadcast(flowsheet_entity_tsv_df),
                             (df[fact_table_join_columns[0]] == flowsheet_entity_tsv_df[flowsheet_tsv_join_cols[0]]) &
                             (df[fact_table_join_columns[1]] == flowsheet_entity_tsv_df[
                                 flowsheet_tsv_join_cols[1]]) &  # dk
                             (df[fact_table_join_columns[2]].eqNullSafe(
                                 flowsheet_entity_tsv_df[flowsheet_tsv_join_cols[2]])) &  # units has null
                             (df[fact_table_join_columns[3]].eqNullSafe(
                                 flowsheet_entity_tsv_df[flowsheet_tsv_join_cols[3]])) &  # type description has null
                             (df[fact_table_join_columns[4]] == flowsheet_entity_tsv_df[flowsheet_tsv_join_cols[4]])
                             # subtype desc
                             )

        if is_categorical_flowsheet_present:
            for col in flowsheet_tsv_join_cols:
                flowsheet_entity_tsv_df_cat = dfutils.normalize_trim_col(flowsheet_entity_tsv_df_cat, col)

            df_cat = df.join(F.broadcast(flowsheet_entity_tsv_df_cat),
                             (df[fact_table_join_columns[0]] == flowsheet_entity_tsv_df_cat[
                                 flowsheet_tsv_join_cols[0]]) &
                             (df[fact_table_join_columns[1]] == flowsheet_entity_tsv_df_cat[
                                 flowsheet_tsv_join_cols[1]]) &  # dk
                             (df[fact_table_join_columns[2]].eqNullSafe(
                                 flowsheet_entity_tsv_df_cat[flowsheet_tsv_join_cols[2]])) &  # units has null
                             (df[fact_table_join_columns[3]].eqNullSafe(
                                 flowsheet_entity_tsv_df_cat[
                                     flowsheet_tsv_join_cols[3]])) &  # type description has null
                             (df[fact_table_join_columns[4]] == flowsheet_entity_tsv_df_cat[
                                 flowsheet_tsv_join_cols[4]]) &
                             # subtype desc
                             (df['FLOWSHEET_RESULT_TXT'] == flowsheet_entity_tsv_df_cat['OriginalToken'])
                             )
        if self.debug:
            print(f'Number of numeric rows post joining with harmonization table: {df_numeric.count()}')
            # print(f'Number of cat rows post joining with harmonization table: {df_cat.count()}')

        # strip and lowercase for numeric
        if not return_df:  # hyc
            df_numeric = dfutils.normalize_trim_col(df_numeric, 'FLOWSHEET_RESULT_TXT')
            df_numeric = df_numeric.withColumn("FLOWSHEET_RESULT_TXT",
                                               round(df_numeric.FLOWSHEET_RESULT_TXT.cast(DoubleType()), 2))
            df_numeric = df_numeric.na.drop(subset=["FLOWSHEET_RESULT_TXT"])  # non double numbers will become: null
        else:  # spark
            df_numeric = dfutils.normalize_trim_col(df_numeric, 'FLOWSHEET_RESULT_TXT')
            df_numeric = df_numeric.withColumn("RESULT", round(df_numeric.FLOWSHEET_RESULT_TXT.cast(DoubleType()), 2))
            df_numeric = df_numeric.na.drop(subset=["RESULT"])
        if self.debug:
            print(f'fact flowsheet size post dropping null or non parsable records for numeric: {df_numeric.count()}')

        if not return_df:
            df_numeric = df_numeric.withColumn("FLOWSHEET_RESULT",
                                               F.when(F.col("Formula") == F.lit("_value*0.01"),
                                                      F.col("FLOWSHEET_RESULT_TXT") * 0.01)
                                               .otherwise(F.col("FLOWSHEET_RESULT_TXT"))
                                               )
        else:
            df_numeric = df_numeric.withColumn("FLOWSHEET_RESULT",
                                               F.when(F.col("Formula") == F.lit("_value*0.01"),
                                                      F.col("RESULT") * 0.01)
                                               .otherwise(F.col("RESULT"))
                                               )

        # filter values within range rangeKnobMax and min
        if syn_config['remove_outliers']:
            df_numeric = df_numeric.filter((
                                        (F.col('IsValidRangeBool') == 'true') &
                                       (F.col("FLOWSHEET_RESULT") <= F.col("RangeKnobMax")) &
                                       (F.col("FLOWSHEET_RESULT") >= F.col("RangeKnobMin"))) |
                                        (F.col('IsValidRangeBool') == 'false')
                                        )

        # add custom ranges
        df_numeric_join_col = 'DisplayName'
        fdf = dict_to_sparkdf(CUSTOM_RANGES_DICT['FACT_FLOWSHEETS'], spark)
        df_numeric = df_numeric.join(F.broadcast(fdf), df_numeric[df_numeric_join_col] == fdf['nfer_varname'],
                                     how='left',
                                     )
        df_numeric = df_numeric.filter(
            (F.col('nfer_varname').isNull()) |
            (
                    (F.col('nfer_varname').isNotNull()) &
                    (F.col("FLOWSHEET_RESULT") <= F.col("range_max")) &
                    (F.col("FLOWSHEET_RESULT") >= F.col("range_min"))
            )
        )

        if return_df:
            return df_numeric, None  # df_cat
        # remove column
        df_numeric = df_numeric.drop(F.col('FLOWSHEET_ROW_DESCRIPTION'))
        df_numeric = df_numeric.withColumnRenamed('DisplayName', 'FLOWSHEET_ROW_DESCRIPTION')

        # compression logic
        patient_window = Window.partitionBy("NFER_PID", "FLOWSHEET_ROW_DESCRIPTION").orderBy("NFER_DTM")
        df_numeric = df_numeric.withColumn("DATE", F.from_unixtime(F.col("NFER_DTM")))
        df_numeric = df_numeric.withColumn("REPEATED",
                                           F.col("FLOWSHEET_RESULT") == F.lag("FLOWSHEET_RESULT").over(patient_window))
        df_numeric = df_numeric.withColumn("DATEDIFF_FROM_PREV",
                                           F.datediff(F.col("DATE"), F.lag("DATE", 1).over(patient_window)))
        df_numeric = df_numeric.na.fill(value=-1, subset=["DATEDIFF_FROM_PREV"])
        df_numeric = df_numeric.na.fill(value=False, subset=["REPEATED"])

        df_numeric = df_numeric.withColumn("DATEDIFF_FROM_PREV",
                                           F.when(F.col("DATEDIFF_FROM_PREV") == F.lit(""), -1)
                                           .otherwise(F.col("DATEDIFF_FROM_PREV"))
                                           )
        df_numeric = df_numeric.withColumn("REPEATED",
                                           F.when(F.col("REPEATED") == F.lit(""), False)
                                           .otherwise(F.col("REPEATED"))
                                           )

        df_numeric = df_numeric.filter(
            ~((F.col('REPEATED') == True) & (F.col("DATEDIFF_FROM_PREV") == 0))
        )
        df_numeric = self.assign_file_id_for_nfer_pid(df_numeric,"DIM_PATIENT")
        if debug_mode:
            print(f'After compression size: {df_numeric.count()}')

        # renaming cols to schema name:
        rename_map = {
            "FLOWSHEET_ROW_DESCRIPTION":"NFER_VARIABLE_NAME",
            "FLOWSHEET_RESULT":"NFER_NORMALISED_VALUE"
        }
        for col in rename_map:
            df_numeric = df_numeric.withColumnRenamed(col,rename_map[col])

        # WRITE DATA
        source = syn_config["dir_name"].split("/")[-1]
        self.write_final_dir("SYN_DATA", df_numeric, nfer_schema, source)
        end = time.perf_counter()
        print("HARMONIZED_FLOWSHEET : Total Time =", end - start)

    def rename_columns(self, df, col_map):
        for old_col, new_col in col_map.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def generate_column_mapping(self):
        self.LABTEST_COL_MAP = {}
        self.FLOWSHEET_COL_MAP = {}

        config_dict = self.SYSTEM_DICT['syn_tables']["harmonized_measurements"]
        flowsheet_cols = config_dict["flowsheet_cols"]
        flowsheet_rename_cols = config_dict["flowsheet_rename_cols"]
        assert (len(flowsheet_cols) == len(flowsheet_rename_cols))

        labtest_cols = config_dict["labtest_cols"]
        labtest_rename_cols = config_dict["labtest_rename_cols"]
        assert (len(labtest_cols) == len(labtest_rename_cols))

        for i in range(len(flowsheet_cols)):
            self.FLOWSHEET_COL_MAP[flowsheet_cols[i]] = flowsheet_rename_cols[i]

        for i in range(len(labtest_cols)):
            self.LABTEST_COL_MAP[labtest_cols[i]] = labtest_rename_cols[i]

    def rename_columns_to_schema(self, df_labtest, df_flowsheet):
        config_dict = self.SYSTEM_DICT['syn_tables']["harmonized_measurements"]
        df_labtest = self.rename_columns(df_labtest, self.LABTEST_COL_MAP)
        if df_flowsheet is not None:
            df_flowsheet = self.rename_columns(df_flowsheet, self.FLOWSHEET_COL_MAP)

        # add missing columns in flowsheet
        if df_flowsheet is not None:
            for col in config_dict['columns_missing_in_flowsheet']:
                if col not in df_flowsheet.columns:
                    df_flowsheet = df_flowsheet.withColumn(col, F.lit(''))
        columns_to_generate = config_dict["columns_to_generate"]

        if self.demo:
            df_labtest = df_labtest.withColumn("VERSION", F.lit("foo")) \
                .withColumn("UPDATED_BY", F.lit("foo")) \
                .withColumn("ROW_ID", F.lit("foo"))
            if df_flowsheet is not None:
                df_flowsheet = df_flowsheet.withColumn("VERSION", F.lit("foo")) \
                    .withColumn("UPDATED_BY", F.lit("foo")) \
                    .withColumn("ROW_ID", F.lit("foo"))

        return df_labtest.select(columns_to_generate), df_flowsheet.select(
            columns_to_generate) if df_flowsheet is not None else None

    def generate_harmonized_combined_summary_spark(self,version=None):
        start = time.perf_counter()

        # generate map of columns to rename to for flowsheet and labtest
        self.generate_column_mapping()
        latest_only  = self.SYSTEM_DICT['syn_tables']["harmonized_measurements"]['take_latest_version_only']
        df_flowsheet, df_cat_flowsheet = self.generate_harmonized_flowsheet_summary_hyc(version=version,
                                                                                        return_df=True,
                                                                                        latest_only=latest_only)
        is_naming_problem = False
        for col in self.FLOWSHEET_COL_MAP:
            if col not in df_flowsheet.columns:  # or col not in df_cat_flowsheet.columns:
                print(f'{col} not in generated df_flowsheet cols\n')  # available columns are {df_flowsheet.columns}')
                is_naming_problem = True
        df_flowsheet = df_flowsheet.withColumn("SOURCE_TABLE", F.lit(SOURCE_TABLE_FLOWSHEET))
        # df_cat_flowsheet = df_cat_flowsheet.withColumn("SOURCE_TABLE", F.lit(SOURCE_TABLE_FLOWSHEET))

        df_labtest, df_cat_labtest = self.generate_harmonized_labtest_summary_hyc(version=version,return_df=True,
                                                                                  latest_only=latest_only)
        df_labtest = df_labtest.withColumn("SOURCE_TABLE", F.lit(SOURCE_TABLE_LABTEST))
        df_cat_labtest = df_cat_labtest.withColumn("SOURCE_TABLE", F.lit(SOURCE_TABLE_LABTEST))

        for col in self.LABTEST_COL_MAP:
            if col not in df_labtest.columns or col not in df_cat_labtest.columns:
                print(f'{col} not in generated df_flowsheet cols\n available columns are {df_labtest.columns}')
                is_naming_problem = True

        if is_naming_problem:
            print("Mismatch between column names mapping")
            raise Exception
        # rename columns to match
        df_labtest, df_flowsheet = self.rename_columns_to_schema(df_labtest, df_flowsheet)
        df_cat_labtest, df_cat_flowsheet = self.rename_columns_to_schema(df_cat_labtest, df_cat_flowsheet)

        if self.debug:
            print(
                f'Size of categorical tables labtest: {df_cat_labtest.count()}')  # flowsheet: {df_cat_flowsheet.count()}')
            print(f'Size of numeric tables labtest: {df_labtest.count()} flowsheet: {df_flowsheet.count()}')

        # union two spark dataframes
        df = df_flowsheet.union(df_labtest)

        if df_cat_flowsheet is None:
            df_cat = df_cat_labtest
        else:
            df_cat = df_cat_flowsheet.union(df_cat_labtest)

        df = df.union(df_cat)

        table_name = "{}.parquet".format(self.SYSTEM_DICT['syn_tables']["harmonized_measurements"]['table_name'])
        if not self.demo:
            out_dir = os.path.join(self.DATA_DIR, "DELTA_TABLES", "SYN_TABLES", table_name)
        else:
            out_dir = os.path.join('/data3/tmp/', table_name)

        print(f'Writing parquet to {out_dir}')
        self.write_df_to_delta_lake(df, out_dir)

        end = time.perf_counter()
        print("HARMONIZED_COMBINED_SUMMARY : Total Time =", end - start)
        return df

    def run(self):
        if 'harmonized_flowsheets' in self.options.run:
            self.generate_harmonized_flowsheet_summary_hyc(latest_only=True)
        if 'harmonized_labtests' in self.options.run:
            self.generate_harmonized_labtest_summary_hyc(latest_only=True)
        if 'harmonized_combined_summary_spark' in self.options.run:
            self.generate_harmonized_combined_summary_spark()


if __name__ == '__main__':
    HarmonizedSummaries(debug=False, demo=False).run()
