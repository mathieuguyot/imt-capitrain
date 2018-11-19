class ICashingStrategy:

    def __init__(self):
        self.storage = None

    def setStorage(self, storage):
        self.storage = storage

    def clearStorage(self, dataset):
        raise Exception("NotImplementedException")

    