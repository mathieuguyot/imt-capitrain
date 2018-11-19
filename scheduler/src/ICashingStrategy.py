class ICashingStrategy:

    def __init__(self):
        pass

    def setStorage(self, storage):
        self.storage = storage

    def clearStorage(self, dataset):
        raise Exception("NotImplementedException")

    