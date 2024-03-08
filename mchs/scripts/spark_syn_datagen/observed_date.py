"""
* Generate for each version, Each table a row where for each patient ID first and last
  Observed date is recorded.
* Should be able to generate from incremental data by looking at the previous table.

* Write a few functions to view the data for synthetic editing.

NFER_PID|ROW_ID|FIRST_OBSERVED_DTM|LAST_OBSERVED_DTM|TABLE_NAME
INTEGER|INTEGER|INTEGER|INTEGER|STRING

* RECORD COUNT IS UNDEFINED FOR THIS COL

* Future thoughts
* we should be able to ignore tables like appointment, patient_referral. Appointments
* ignore results post death date
* event they update a default which should be ignored. These defaults we noticed in the past are usually
1000-01-01
9999-12-31
1900-01-01
"""

import os
from time import time
from pyspark.sql import functions as F, DataFrame
from spark_syn_datagen.synthetic_summaries import SynDataGenJob

from pyspark.sql.types import IntegerType
from core.config_vars import RunMode

ALL_TABLES_STR = "ALL_TABLES"
HYC_TABLES_STR = "HYC_TABLES"


class ObservedDate(SynDataGenJob):

    def __init__(self, debug=False, demo=False):
        super().__init__()
        self.debug = debug
        self.demo = demo

        self.out_table = 'observed_date_summary'
        self.syn_config = self.SYSTEM_DICT["syn_tables"][self.out_table]
        self.fact_tables_list = [self.SYSTEM_DICT['data_tables'][table]['dir_name'].split("/")[-1] for table in
                                 self.SYSTEM_DICT['data_tables'] if table.startswith('fact_')]
        self.fact_table_petnames_list = [table for table in
                                         self.SYSTEM_DICT['data_tables'] if table.startswith('fact_')]
        self.tables_without_nfer_dtm = ['FACT_GENOMIC_BIOSPECIMEN_FLAGS', 'FACT_PATHOLOGY_EXTENDED_REPORT',
                                        'FACT_PATHOLOGY_SPECIMEN_DETAIL', ]
        self.spark = self.init_spark_session()

        if self.debug:
            print("** Running with debug mode on, will read a subset of data **")

    def get_first_last_ts_df(self, df:DataFrame, table):
        """
        For each pid get a df of first and last observed date
        subject to some filters
        """
        df.cache()
        df = df.withColumn("NFER_DTM", df["NFER_DTM"].cast(IntegerType()))
        # df = df.withColumn("NFER_AGE", df["NFER_AGE"].cast(IntegerType()))
        # apply start filter
        # df = df.filter(F.col("NFER_AGE") >= 0)

        # apply end filter where timestamp > last data release date
        # df = df.filter(F.col("NFER_DTM") <= get_cutoff_date(version))

        df = df.groupBy("NFER_PID").agg(
            F.min("NFER_DTM").alias("FIRST_OBSERVED_DTM"),
            F.max("NFER_DTM").alias("LAST_OBSERVED_DTM"),
        )

        df = df.withColumn('TABLE_NAME', F.lit(table))

        return df

    def addon_extra_rows_for_aggregate(self, df:DataFrame):
        """
        Given a table of pid,first_observed_date,last_observed_date,tablename
        adds for each pid 2 rows,
        one for all over max and min ts
        * planned another for hyc min max where only hybrid cohorts tables are considered
        """
        add_on_df = df.groupBy("NFER_PID").agg(
            F.min("FIRST_OBSERVED_DTM").alias("FIRST_OBSERVED_DTM"),
            F.max("LAST_OBSERVED_DTM").alias("LAST_OBSERVED_DTM")
        )
        add_on_df = add_on_df.withColumn('TABLE_NAME', F.lit(ALL_TABLES_STR))
        df = df.union(add_on_df)
        df.show(3)
        return df

    ###
    def get_current_version_table(self, version):
        """
        given the records for this version,
        generate the table for this version only
        considers only latest record
        """

        final_df = None
        failed_table_list = []
        all_fact_tables = self.fact_tables_list
        if self.debug:
            all_fact_tables = all_fact_tables[:2]
        for table in all_fact_tables:
            if table not in self.tables_without_nfer_dtm:
                try:
                    print(f"Processing Table V{version} {table}")

                    df = self.read_versioned_datagen_dir(
                        table,
                        columns=['NFER_PID', 'NFER_DTM'],
                        latest_only=True,
                        version=version,
                    )

                    df = self.get_first_last_ts_df(df, table)
                    print(f"Record count in observed_dtm table for {table}: {df.count()}")
                    if final_df is None:
                        final_df = df
                    else:
                        final_df = final_df.union(df)
                except:
                    print("Error occured for {table}")
                    failed_table_list.append(table)

        print(f"Before adding extra rows for aggregate: {final_df.count()}")
        final_df = self.addon_extra_rows_for_aggregate(final_df)
        print(f"After adding extra rows for aggregate: {final_df.count()}")

        if failed_table_list:
            print(f"Output table is generated but generation failed for the following tables {failed_table_list}")

        return final_df

    def run(self):
        """
        Takes only the latest records in this version and generates the table for this version
        """
        version = self.RUN_VERSION
        final_df = self.get_current_version_table(version)
        self.write_versioned_syn_data(
            syn_config=self.syn_config,
            syn_data_df=final_df,
            version=version)



if __name__ == '__main__':
    start = time()
    obj = ObservedDate(debug=False)
    obj.run()

    print("{} complete! : Total Time = {} Mins".format(obj.out_table, round((time() - start) / 60)))
