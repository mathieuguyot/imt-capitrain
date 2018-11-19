from batsim.batsim import Job
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

        # Attach dataset to target storage
        to_storage.addDataset(dataset)

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
