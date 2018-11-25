from collections import defaultdict
from pprint import pprint

class FloydWarshall:

    def getCost(self, from_storage_id, to_storage_id):
        self.dist[str(from_storage_id)][str(to_storage_id)]

    def getShortestPath(self, from_storage_id, to_storage_id):
        self.path[str(from_storage_id)][str(to_storage_id)]

    def __init__(self, time_exchange_data_dict, storage_id_array):
        self.next = defaultdict(dict)
        self.dist = defaultdict(dict)
        self.path = defaultdict(dict)
        
        # Init dist and nex twoD array
        for job_id, data in time_exchange_data_dict.items():
            print(str(data.from_storage_id) + " -> " + str(data.to_storage_id) + " : " + str(data.time))
            self.dist[str(data.from_storage_id)][str(data.to_storage_id)] = data.time
            self.next[str(data.from_storage_id)][str(data.to_storage_id)] = data.to_storage_id

        # FW main computation
        for k in storage_id_array:
            for i in storage_id_array:
                for j in storage_id_array:
                    cost = self.dist[str(i)][str(k)] + self.dist[str(k)][str(j)]
                    if float(self.dist[str(i)][str(j)]) > float(cost):
                        self.dist[str(i)][str(j)] = cost
                        self.next[str(i)][str(j)] = self.next[str(i)][str(k)]

        # FW path reconstruction
        for i in storage_id_array:
            for j in storage_id_array:
                if i != j:
                    path = [i]
                    while path[-1] != j:
                        path.append(self.next[str(path[-1])][str(j)])
                    self.path[str(i)][str(j)] = path
        
        pprint(self.path)
        