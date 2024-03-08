import os
from time import time
from pyspark.sql import functions as F
from pyspark.sql import Window
from spark_syn_datagen.synthetic_summaries import SynDataGenJob
from pyspark.sql import SparkSession, udf
from pyspark.sql.types import ArrayType, FloatType, StringType, IntegerType, DateType, LongType
from pyspark.sql.functions import upper, lower, when, lit, col, split, explode, datediff, broadcast, create_map
from itertools import chain

adt_inp_list = ['inpatient',
                'pre-admit to inpatient',
                'emergency to inpatient',
                'direct admit as inpatient',
                'outpatient to inpatient',
                'ip transitional care',
                'ip behavioral health',
                'ip rehab',
                'ip hospice']

CPT_CODE_DICT = {
    '99221': "initial hospital",
    '99222': "initial hospital",
    '99223': "initial hospital",
    '99231': "subsequent hospital",
    '99232': "subsequent hospital",
    '99233': "subsequent hospital",
    '99238': "discharge",
    '99239': "discharge",
    '99281': "emergency",
    '99282': "emergency",
    '99283': "emergency",
    '99284': "emergency",
    '99285': "emergency",
    '99291': "critical care",
    '99292': "critical care"
}

DIM_PAT_HEADERS = ["NFER_PID", "BIRTH_DATE", "FILE_ID", "PATIENT_MERGED_FLAG"]
FACT_ADT_HEADERS = ["NFER_PID", "ADMIT_DTM", "DISCHARGE_DTM", "PATIENT_PROCESS_TYPE"]
FACT_PROC_HEADERS = ["NFER_PID", "NFER_DTM", "PROCEDURE_CODE"]

HOSP_DATA_HEADERS = ['NFER_PID', 'NFER_DTM', 'TAG', 'TYPE', 'TABLE']
HOSP_SUMMARY_HEADERS = ['NFER_PID', 'CYCLE_NUM', 'DAYS', 'ADMIT_DTM', 'DISCHARGE_DTM', 'ADMIT_AGE', 'DISCHARGE_AGE']


def get_cycles(dtm_l,tag_l, table_l):
    """
    Cycles is a four tuple of
    (cycle_index,len_cycle,start,end)
    """
    cycles = []
    max_ind = len(dtm_l)-1
    start, end, start_table = -1, -1, ''
    for ind, (dtm, tag, table) in enumerate(zip(dtm_l, tag_l, table_l)):

        if ind <= end:
            continue

        if tag == 1:
            # if there is alreay a initial_hosp and not from adt table, or big time difference bw initial hospitals
            if (start < 0 or start_table != 'ADT') or (dtm_l[ind]-dtm_l[start])//86400 > 30:
                start, start_table = ind, table

        if tag == -1:
            # if  discharge is from same table having init_hosp
            if start >= 0 and start_table == table:

                end = ind

                # move start point back as far as possible if you get initial or subsequent hospitalization
                # while we get initial or subsequent hosp within two days of initial start, move back start
                while start > 0 and dtm_l[start]-dtm_l[start-1] <= 2*86400 and tag_l[start-1] in [0, 1]:
                    start -= 1
                while end < max_ind and dtm_l[end+1]-dtm_l[end] <= 2*86400 and tag_l[end+1] in [0, -1]:
                    end += 1

                cycle_len = (dtm_l[end]-dtm_l[start])//86400+1
                if cycle_len > 0:
                    # merging overlapping cycles
                    # while current start <= last cycle end
                    if cycles and dtm_l[start] <= cycles[-1][3]:
                        cycles[-1][3] = dtm_l[end] # make last cycle end equal to this
                        cycles[-1][1] = (dtm_l[end]-cycles[-1][2])//86400+1
                    else:
                        cycles.append([len(cycles)+1, cycle_len, dtm_l[start], dtm_l[end]])
                start, end, start_table = -1, -1, ''

    return cycles


def get_ccu_cycles(dtm_list):

    cycles = []

    if not dtm_list:
        return cycles

    start, end = -1, -1
    for i, dtm in enumerate(sorted(dtm_list)):
        if start == -1:
            start, end = dtm, dtm
        elif dtm-dtm_list[i-1] <= 86400:
            end = dtm
        else:
            cycle_len = (end - start)//86400 + 1
            cycles.append([len(cycles)+1, cycle_len, start, end])
            start, end = dtm, dtm

    cycle_len = (end - start)//86400 + 1
    cycles.append([len(cycles)+1, cycle_len, start, end])
    return cycles


class Hospitalization(SynDataGenJob):

    def __init__(self):
        super().__init__()

    def write_summary_df(self, df, dim_df, out_config, spark):

        df = df.select(df["NFER_PID"], explode(df['CYCLES']))
        df = df.select("NFER_PID", df.col[0], df.col[1], df.col[2], df.col[3]).toDF('NFER_PID', 'CYCLE', 'DAYS',
                                                                                    'ADMIT_DTM', 'DISCHARGE_DTM')

        # adding age columns
        df = df.join(broadcast(dim_df), on=['NFER_PID'], how='left')
        df = df.withColumn("ADMIT_AGE", (datediff(F.from_unixtime('ADMIT_DTM').cast(DateType()), col("BIRTH_DATE")) / 365.25).cast(IntegerType()))
        df = df.withColumn("DISCHARGE_AGE", (datediff(F.from_unixtime('DISCHARGE_DTM').cast(DateType()), col("BIRTH_DATE")) / 365.25).cast(IntegerType()))
        df = df.drop('BIRTH_DATE')

        self.write_versioned_syn_data(out_config, df)

    def generate_hosp_summary(self, config_hosp_data, config_hosp_summary, config_ccu_summary):

        spark: SparkSession = self.init_spark_session()

        # loading birthdate from dim patient files
        dim_df = self.read_versioned_datagen_dir("DIM_PATIENT", DIM_PAT_HEADERS)
        dim_df = self.de_dupe_patient_meta(dim_df)
        dim_df.cache()

        # adt table creation
        adt_df = self.read_versioned_datagen_dir("FACT_ADMIT_DISCHARGE_TRANSFER_LOCATION", FACT_ADT_HEADERS)
        adt_df = adt_df.distinct()
        adt_df = adt_df.withColumnRenamed('PATIENT_PROCESS_TYPE', 'TYPE')
        adt_df = adt_df.filter(lower(adt_df['TYPE']).isin(adt_inp_list))

        # adt admission table
        adt_in_df = adt_df.select(['NFER_PID', 'ADMIT_DTM', 'TYPE'])
        adt_in_df = adt_in_df.withColumn('TAG', lit('initial hospital'))
        adt_in_df = adt_in_df.withColumnRenamed('ADMIT_DTM', 'NFER_DTM').dropna()

        # adt discharge table
        adt_out_df = adt_df.select(['NFER_PID', 'DISCHARGE_DTM', 'TYPE'])
        adt_out_df = adt_out_df.withColumn('TAG', lit('discharge'))
        adt_out_df = adt_out_df.withColumnRenamed('DISCHARGE_DTM', 'NFER_DTM').dropna()

        # combining adt admission and discharge table
        adt_df = adt_in_df.union(adt_out_df).withColumn('TABLE', lit('ADT'))

        # procedure table
        proc_df = self.read_versioned_datagen_dir("FACT_PROCEDURES", FACT_PROC_HEADERS)
        proc_df = proc_df.distinct()
        proc_df = proc_df.filter(proc_df['PROCEDURE_CODE'].isin(list(CPT_CODE_DICT.keys())))
        mapping_expr = create_map([lit(x) for x in chain(*CPT_CODE_DICT.items())])
        proc_df = proc_df.withColumn("TAG", mapping_expr.getItem(col("PROCEDURE_CODE")))
        proc_df = proc_df.withColumnRenamed("PROCEDURE_CODE", 'TYPE')
        proc_df = proc_df.withColumn('TABLE', lit('PROCEDURE'))
        proc_df.cache()

        # create hospitalization data table by combineing adt and procedure table
        df = adt_df.union(proc_df)
        df = df.dropDuplicates(["NFER_PID", "NFER_DTM", "TAG", "TABLE"])
        df = df.withColumn("DATE", F.from_unixtime('NFER_DTM').cast(DateType()))
        df = df.withColumn("NFER_DTM", df["NFER_DTM"].cast(IntegerType()))
        df = df.select("NFER_PID", "NFER_DTM", "DATE", "TAG", "TYPE", "TABLE")
        df = df.orderBy(["NFER_PID", "NFER_DTM"])
        df.cache()

        hosp_df = df.join(broadcast(dim_df.drop('BIRTH_DATE')), on=['NFER_PID'], how='left')
        self.write_versioned_syn_data(config_hosp_data, hosp_df)

        # only considering following types for cycle calculation
        sum_df = df.filter(df["TAG"].isin(['initial hospital', 'subsequent hospital', 'discharge']))
        sum_df = sum_df.replace(['subsequent hospital', 'initial hospital', 'discharge'], ['0', '1', '-1'], 'TAG')
        sum_df = sum_df.withColumn("TAG", sum_df["TAG"].cast(IntegerType()))

        # create hospitalization summary table
        """
        sum_df = sum_df.groupby('NFER_PID').agg(F.expr('collect_list(NFER_DTM)').alias('NFER_DTM'),
                                    F.expr('collect_list(DATE)').alias('DATE'),
                                    F.expr('collect_list(TAG)').alias('TAG'),
                                    F.expr('collect_list(TABLE)').alias('TABLE'))
        """
        w = Window.partitionBy('NFER_PID').orderBy('NFER_DTM')

        sum_df = sum_df.withColumn('NFER_DTM', F.collect_list('NFER_DTM').over(w)) \
            .withColumn('TAG', F.collect_list('TAG').over(w)) \
            .withColumn('TABLE', F.collect_list('TABLE').over(w))

        sum_df = sum_df.groupBy('NFER_PID').agg(F.max('NFER_DTM').alias('NFER_DTM'),
                                          F.max('TAG').alias('TAG'),
                                          F.max('TABLE').alias('TABLE'))

        convert = F.udf(get_cycles, ArrayType(ArrayType(IntegerType())))
        sum_df = sum_df.withColumn('CYCLES', convert(sum_df["NFER_DTM"], sum_df['TAG'], sum_df["TABLE"]))
        self.write_summary_df(sum_df, dim_df, config_hosp_summary, spark)

        # create ccu summary table
        ccu_df = proc_df.filter(proc_df["TAG"] == 'critical care').select('NFER_PID', 'NFER_DTM')
        ccu_df = ccu_df.withColumn("NFER_DTM", ccu_df["NFER_DTM"].cast(IntegerType()))
        ccu_df = ccu_df.orderBy(['NFER_PID', 'NFER_DTM'])
        ccu_df = ccu_df.groupby('NFER_PID').agg(F.expr('collect_list(NFER_DTM)').alias('NFER_DTM'))
        convert = F.udf(get_ccu_cycles, ArrayType(ArrayType(IntegerType())))
        ccu_df = ccu_df.withColumn('CYCLES', convert(ccu_df["NFER_DTM"]))
        self.write_summary_df(ccu_df, dim_df, config_ccu_summary, spark)


if __name__ == '__main__':

    out_table = 'death_summary'

    start = time()
    obj = Hospitalization()
    config_hosp_data = obj.SYSTEM_DICT["syn_tables"]['hospitalization_data']
    config_hosp_summary = obj.SYSTEM_DICT["syn_tables"]['hospitalization_summary']
    config_ccu_summary = obj.SYSTEM_DICT["syn_tables"]['ccu_summary']
    obj.generate_hosp_summary(config_hosp_data, config_hosp_summary, config_ccu_summary)
    print("Hospitalization summary compelte! : Total Time = {} Mins".format(round((time() - start)/60)))
