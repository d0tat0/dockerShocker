# define this as abstract class
#
from abc import ABC, abstractmethod
from core.core_base import CoreBase
import os
from dotmap import DotMap
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from spark_cohort_datagen.table_db import TableDB

ALL_TABLETYPE_KEY_NAMES = ["info_tables", "data_tables", "syn_tables"]


class DataSet:
    def __init__(
        self,
        dflist,
        name_list,
    ):
        self._dflist = dflist
        self._name_list = name_list

    def get_dflist(self):
        return self._dflist

    def get_name_list(self):
        return self._name_list

    def get_data_as_dict(self):
        return dict(zip(self._name_list, self._dflist))


class DataGen(ABC):
    def run(self):
        dataset_obj = self.get_data()
        dataset_obj = self.transform_data(dataset_obj)
        dataset_obj = self.output_data(dataset_obj)
        return dataset_obj

    @abstractmethod
    def transform_data(self, data_obj: DataSet) -> DataSet:
        # all the transformation no output
        # read
        pass

    @abstractmethod
    def output_data(self, data_obj: DataSet) -> DataSet:
        # write to some db,location, etc
        pass

    @abstractmethod
    def get_data(self) -> DataSet:
        # through hook or crook create data
        # generate or read from some location
        pass

    @abstractmethod
    def validate(self) -> DotMap:
        pass

    def __init__(self, table_name, version, raw_data_path, write_base_path):
        self._read_path = None
        self._write_base_path = None
        self._table_name = None
        self._version = None
        self._orch_config = None
        self._table_schema = None
        self._write_path = None
        self._table_db = None
        self._spark = None

        self.set_table_name(table_name)
        self.set_version(version)
        self.set_raw_data_path(raw_data_path)
        self.set_write_base_path(write_base_path)

        self._orch_config = CoreBase(None)
        self._orch_config.get_dirs()
        self._table_db = TableDB()
        self._table_schema = None

        for key in ALL_TABLETYPE_KEY_NAMES:
            try:
                self._table_schema = self._orch_config.SYSTEM_DICT[key][
                    self._table_name
                ]["nfer_schema"]
            except KeyError:
                pass
            if self._table_schema is not None:
                break

        self._spark = SparkSession.builder.appName(
            f"{self.get_table_name()}_{self.get_version()}_datgen"
        ).getOrCreate()
        self._write_path = (
            os.path.join(self._write_base_path, self._version, self._table_name)
            + ".parquet"
        )

    def set_table_name(self, table_name):
        if table_name is None:
            raise NotImplementedError("table_name is None")
        self._table_name = table_name.upper()

    def get_table_name(self):
        return self._table_name

    def set_version(self, version):
        self._version = version

    def get_version(self):
        return self._version

    def set_raw_data_path(self, raw_data_path):
        if raw_data_path is None:
            raise NotImplementedError("raw_data_path is None")
        self._read_path = raw_data_path

    def get_raw_data_path(self):
        return self._read_path

    def set_write_base_path(self, write_base_path):
        if write_base_path is None:
            raise NotImplementedError("write_base_path is None")
        self._write_base_path = write_base_path

    def get_write_base_path(self):
        return self._write_base_path

    def set_orch_config(self, orch_config):
        self._orch_config = orch_config

    def get_orch_config(self):
        return self._orch_config

    def set_table_schema(self, table_schema):
        self._table_schema = table_schema

    def get_table_schema(self):
        return self._table_schema

    def set_write_path(self, write_path):
        self._write_path = write_path

    def get_write_path(self):
        return self._write_path

    def set_table_db(self, table_db):
        self._table_db = table_db

    def get_table_db(self):
        return self._table_db

    def set_spark(self, spark):
        self._spark = spark

    def get_spark(self):
        return self._spark

    def read_delta_table(self):
        return self.__read_delta_table_core(
            self.get_spark(), self.get_table_name(), self.get_version()
        )

    def __read_delta_table_core(self, spark, table_name, version):
        table_name = table_name.upper()
        path = self.get_table_db().get_delta_path_by_id(table_name, version)
        print(f"Reading from {path}")
        df = spark.read.parquet(path)
        return df

    def add_version_and_updated_by_cols(self, df, version=5.029):
        version = round(float(version), 3)
        df = df.withColumn("VERSION", F.lit(version))
        df = df.withColumn("UPDATED_BY", F.lit(0.0))
        return df
