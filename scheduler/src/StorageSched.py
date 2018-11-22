from batsim.batsim import BatsimScheduler, Batsim, Job

import sys
import time
import os
import logging
from pprint import pprint

from LRUCashingStrategy import LRUCashingStrategy
from StorageCtrl import StorageCtrl
from Storage import Storage
from Dataset import Dataset

class StorageSched(BatsimScheduler):

    def myprint(self,msg):
        print("[{time}] {msg}".format(time=self.bs.time(), msg=msg))

    def __init__(self, options):
        self.options = options
        self.prev_time = 0

    def onSimulationBegins(self):
        self.storageCtrl = StorageCtrl(self.bs)
        self.bs.logger.setLevel(logging.ERROR)

        for machine in self.bs.machines["storage"]:
            id = machine["id"]
            name = machine["name"]
            byte_size = int(machine["properties"]["byte_size"])
            storage = Storage(id, name, byte_size, LRUCashingStrategy())
            self.storageCtrl.addStorage(storage)

        # End the simulation
        self.bs.wake_me_up_at(1000)

    def onJobSubmission(self, job):
        pass

    def onJobCompletion(self, job):
        time = job.finish_time - 1000
        time -= self.prev_time
        self.prev_time += time
        self.storageCtrl.addTimeExchangeData(job.id, time)
        self.myprint("Job_finished: " + job.id)
        self.storageCtrl.printTimeExchangeData()
        self.storageCtrl.displayGraph()

    def onNoMoreEvents(self):
        pass

    def onRequestedCall(self):
        self.storageCtrl.generateTimeExchangeData()
        self.bs.notify_submission_finished()