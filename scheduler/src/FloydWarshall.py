from collections import defaultdict
from pprint import pprint

class FloydWarshall:

    def __init__(self, time_exchange_data_dict, storage_id_array):
        self.next = defaultdict(dict)
        self.dist = defaultdict(dict)
        pprint(storage_id_array)
        # Init dist and nex twoD array
        for job_id, data in time_exchange_data_dict.items():
            self.dist[str(data.from_storage_id)][str(data.to_storage_id)] = data.time
            self.next[str(data.from_storage_id)][str(data.to_storage_id)] = data.to_storage_id

        pprint(self.dist)
        pprint(self.next)

        for k in storage_id_array:
            for i in storage_id_array:
                for j in storage_id_array:
                    if self.dist[str(i)][str(j)] > self.dist[str(i)][str(k)] + self.dist[str(k)][str(j)]:
                        self.dist[str(i)][str(j)] = self.dist[str(i)][str(k)] + self.dist[str(k)][str(j)]
                        self.next[str(i)][str(j)] = self.next[str(i)][str(k)]

        print("ici")
        pprint(self.dist)
        pprint(self.next)