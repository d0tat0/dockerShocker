from abc import abstractmethod
from pyspark.sql import DataFrame
from spark_jobs.data_reader import DataReader

class BaseRawDataReader:

    def __init__(self, data_reader, config_file=None):
        self.data_reader_obj: DataReader = data_reader
        self.data_reader_obj.init_spark_session()

    @abstractmethod
    def read_raw_data(self, source, source_path, version):
        pass

    @abstractmethod
    def read_raw_dim_source(self, source):
        pass
