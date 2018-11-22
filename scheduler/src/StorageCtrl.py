from batsim.batsim import Job
from procset import ProcSet

from Dataset import Dataset
from TimeExchangeData import TimeExchangeData

from pprint import pprint

class StorageCtrl:

    def __init__(self, bs): 
        self.storage_dict = dict()
        self.idSub = 0
        self.bs = bs
        # Defining arguments to help time exchange data generation
        self.is_generating_time_exchange_data = False
        self.time_exchange_data_job_dict = dict()

    def addStorage(self, storage):
        self.storage_dict[storage.id] = storage

    def generateTimeExchangeData(self):
        self.is_generating_time_exchange_data = True
        i = 1

        for source_storage_key, source_storage in self.storage_dict.items():
            dataset_id = "time_exchange_data" + str(i)
            ds = Dataset(dataset_id, 1000000)
            i += 1
            source_storage.addDataset(ds)

            for target_storage_key, target_storage in self.storage_dict.items():
                if target_storage_key != source_storage_key:
                    self.moveDataset(dataset_id, source_storage_key, target_storage_key)

        self.is_generating_time_exchange_data = False

    def addTimeExchangeData(self, job_id, time):
        self.time_exchange_data_job_dict[job_id].time = time

    def printTimeExchangeData(self):
        for key, value in self.time_exchange_data_job_dict.items():
            pprint(value.time)

    def moveDataset(self, dataset_id, from_storage_id, to_storage_id):
        profile_name = "commUP" + str(self.idSub)

        from_storage = self.storage_dict[from_storage_id]
        to_storage = self.storage_dict[to_storage_id]
        dataset = from_storage.getDataset(dataset_id)
        if dataset is None:
            raise "Dataset is not contained in source storage"

        # Attach dataset to target storage
        to_storage.addDataset(dataset)

        # Prepare profile
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

        # Saving job information in case of time exchange data generation
        if self.is_generating_time_exchange_data:
            self.time_exchange_data_job_dict[jid1] = TimeExchangeData(jid1, from_storage_id, to_storage_id)

        # Run job
        self.bs.execute_jobs([job1])

