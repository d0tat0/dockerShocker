import multiprocessing
from core.core_base import CoreBase


class UtilWorker(multiprocessing.Process, CoreBase):
    def __init__(self, i, parent):
        multiprocessing.Process.__init__(self)
        self.worker_id = i
        self.worker_parent = parent
        self.options = parent.options
        self.DATA_DIR = parent.DATA_DIR

    def run(self):
        pass