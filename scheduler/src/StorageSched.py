from batsim.batsim import BatsimScheduler, Batsim, Job

import sys
import os
import logging

from LRUCashingStrategy import LRUCashingStrategy
from StorageCtrl import StorageCtrl
from Storage import Storage
from Dataset import Dataset

class StorageSched(BatsimScheduler):

    def myprint(self,msg):
        print("[{time}] {msg}".format(time=self.bs.time(), msg=msg))

    def __init__(self, options):
        self.options = options

    def onSimulationBegins(self):
        self.storageCtrl = StorageCtrl(self.bs)
        self.bs.logger.setLevel(logging.ERROR)

        for machine in self.bs.machines["storage"]:
            id = machine["id"]
            name = machine["name"]
            byte_size = int(machine["properties"]["byte_size"])
            storage = Storage(id, name, byte_size, LRUCashingStrategy())
            self.storageCtrl.addStorage(storage)
            
        # Here we run a test
        amount = 1024
        ds = Dataset(1, amount * 1024*1024)
        self.storageCtrl.storage_dict[12].addDataset(ds)
        self.storageCtrl.moveDataset(1, 12, 14)
        self.bs.notify_submission_finished()

        self.bs.wake_me_up_at(1000)

    def onJobSubmission(self, job):
        pass

    def onJobCompletion(self, job):
        self.myprint("Job_finished: " + job.id)

    def onNoMoreEvents(self):
        pass

    def onRequestedCall(self):
        pass