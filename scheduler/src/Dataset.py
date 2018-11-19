import datetime

class Dataset:

    def __init__(self, id, byte_size):
        self.id = id
        self.byte_size = byte_size
        self.timestamp = datetime.datetime.now()

    def cloneAndRefreshTimestamp(self):
        return Dataset(self.id, self.byte_size)