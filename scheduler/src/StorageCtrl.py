from batsim.batsim import Job
from procset import ProcSet

from FloydWarshall import FloydWarshall
from Dataset import Dataset
from TimeExchangeData import TimeExchangeData
import tkinter
from pprint import pprint
from networkx import networkx as nx
import  matplotlib.pyplot as plt

class StorageCtrl:

    def __init__(self, bs): 
        self.storage_dict = dict()
        self.storage_ids_arr = []
        self.idSub = 0
        self.bs = bs
        self.fw = None
        # Defining arguments to help time exchange data generation
        self.is_generating_time_exchange_data = False
        self.time_exchange_data_job_dict = dict()

    def addStorage(self, storage):
        self.storage_dict[storage.id] = storage
        self.storage_ids_arr.append(storage.id)

    def generateTimeExchangeData(self):
        self.is_generating_time_exchange_data = True
        i = 1

        for source_storage_key, source_storage in self.storage_dict.items():
            self.time_exchange_data_job_dict[source_storage_key] = TimeExchangeData(source_storage_key, source_storage.id, source_storage.id)
            self.time_exchange_data_job_dict[source_storage_key].time = 0

            dataset_id = "time_exchange_data" + str(i)
            ds = Dataset(dataset_id, 1000000)
            i += 1
            source_storage.addDataset(ds)

            for target_storage_key, target_storage in self.storage_dict.items():
                if target_storage_key != source_storage_key:
                    self.moveDataset(dataset_id, source_storage_key, target_storage_key)

        self.is_generating_time_exchange_data = False

    def addTimeExchangeData(self, job_id, time):
        if job_id not in self.time_exchange_data_job_dict:
            return
        self.time_exchange_data_job_dict[job_id].time = time
        # Check if all time exchange data are set
        areAllTimeExchangeDataSet = True
        for k, v in self.time_exchange_data_job_dict.items():
            if v.time == -1 and areAllTimeExchangeDataSet:
                areAllTimeExchangeDataSet = False

        # Display a graph of the network
        if areAllTimeExchangeDataSet:
            #self.displayGraph()

            # Compute floyd warshall with computed network graph
            self.fw = FloydWarshall(self.time_exchange_data_job_dict, self.storage_ids_arr)
            


    def printTimeExchangeData(self):
        for key, value in self.time_exchange_data_job_dict.items():
            if value.time != 0:
                pprint('from : '+str(value.from_storage_id) + '  to : '+str(value.to_storage_id))
                pprint(value.time)

    def displayGraph(self):
        G = nx.DiGraph()
        for key, value in self.time_exchange_data_job_dict.items():
            if value.time != 0:
                if not G.has_node(value.from_storage_id):
                    G.add_node(value.from_storage_id)
                if not G.has_node(value.to_storage_id):
                    G.add_node(value.to_storage_id)
                G.add_edge(value.from_storage_id, value.to_storage_id, weight=value.time)
        pos = nx.spring_layout(G, scale=2)
        nx.draw(G, pos,  with_labels=True)
        labels = nx.get_edge_attributes(G, 'weight')
        nx.draw_networkx_edge_labels(G, pos , edge_labels= labels)
        plt.savefig('Graph'+key+'.png', format="PNG")
        plt.show()

    def moveDatasetAutomatic(self, dataset_id, to_storage_id):
        if self.fw is None:
            raise "Floyd Warshall not computed, can't move dataset automatically"

        to_storage = self.storage_dict[to_storage_id]
        from_storages = []

        # Find potentials sources storages
        for target_storage_key, target_storage in self.storage_dict.items():
            if target_storage.getDataset(dataset_id) is not None:
                from_storages.append(target_storage_key)
                # Check that target storage does not already contains the dataset
                if target_storage_key == to_storage_id:
                    return

        if from_storages.count is 0:
            raise "No storage contains provided dataset"

        # Use FW to find best source storage
        best_cost = None
        best_from_storage_id = None
        for from_storage_id in from_storages:
            cost = self.fw.getCost(from_storage_id, to_storage_id)
            if best_cost is None:
                best_cost = cost
                best_from_storage_id = from_storage_id
            elif best_cost > cost:
                best_cost = cost
                best_from_storage_id = from_storage_id
        
        movePath = self.fw.getShortestPath(best_from_storage_id, to_storage_id)
        # Perform the move
        for i in range(len(movePath)):
            if i+1 == len(movePath):
                break
            self.moveDataset(dataset_id, movePath[i], movePath[i+1])
            
        

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

