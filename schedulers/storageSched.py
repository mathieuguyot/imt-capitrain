from batsim.batsim import BatsimScheduler, Batsim, Job

import sys
import os
import logging
from procset import ProcSet

class StorageCtrl:

    def __init__(self, bs): 
        self.storage_dict = dict()
        self.idSub = 0
        self.bs = bs

    def addStorage(self, storage):
        self.storage_dict[storage.id] = storage

    def moveDataset(self, dataset_id, from_storage_id, to_storage_id):

        from_storage = self.storage_dict[from_storage_id]
        to_storage = self.storage_dict[to_storage_id]
        dataset = from_storage.getDataset(dataset_id)
        if dataset is None:
            raise "Dataset is not contained in source storage"

        # Prepare profile
        profile_name = "commUP2"
        to_name = self.storage_dict[to_storage_id].name
        from_name = self.storage_dict[from_storage_id].name
        move_profile = {
            profile_name : 
            {
                'type' : 'data_staging', 
                'nb_bytes' : dataset.byte_size, 
                'from' : from_name, 
                'to' : to_name
            },
        }
        self.bs.submit_profiles("dyn", move_profile)

        # Prepare job
        jid1 = "dyn!" + str(self.idSub)
        self.idSub += 1
        self.bs.submit_job(id=jid1, res=1, walltime=-1, profile_name=profile_name)
        job1 = Job(jid1, 0, -1, 1, "", "", "")
        job1.allocation = ProcSet(from_storage_id)
        job1.storage_mapping = {}
        job1.storage_mapping[to_name] = to_storage_id
        job1.storage_mapping[from_name] = from_storage_id

        # Run job
        self.bs.execute_jobs([job1])

        # Attach dataset to target storage
        to_storage.addDataset(dataset)


class Storage:

    def __init__(self, id, name, storage_byte_size):
        self.id = id
        self.name = name
        self.storage_byte_size = storage_byte_size
        self.used_storage_byte_size = 0
        self.dataset_dict = dict()

    def getAvailableSpace(self):
        return self.storage_byte_size - sefl.used_storage_byte_size

    def getTotalSpace(self):
        return self.storage_byte_size
    
    def addDataset(self, dataset):
        # Check that there is enouth space to add dataset
        new_byte_size = self.used_storage_byte_size + dataset.byte_size
        if self.storage_byte_size < new_byte_size:
            raise "No enouth space to add this dataset"
        self.dataset_dict[dataset.id] = dataset
        self.used_storage_byte_size = new_byte_size

    def getDataset(self, id_dataset):
        if id_dataset in self.dataset_dict:
            return self.dataset_dict[id_dataset]
        else:
            return None

class Dataset:

    def __init__(self, id, byte_size):
        self.id = id
        self.byte_size = byte_size

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
            storage = Storage(id, name, byte_size)
            self.storageCtrl.addStorage(storage)
            
        self.bs.wake_me_up_at(1000)

    def onJobSubmission(self, job):
        pass

    def onJobCompletion(self, job):
        self.myprint("Job_finished: " + job.id)

    def onNoMoreEvents(self):
        pass

    def onRequestedCall(self):
        # Here we run a test
        ds = Dataset(1, 100000)
        self.storageCtrl.storage_dict[13].addDataset(ds)
        self.storageCtrl.moveDataset(1, 13, 14)
        self.bs.notify_submission_finished()