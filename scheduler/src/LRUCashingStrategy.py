from ICashingStrategy import ICashingStrategy

class LRUCashingStrategy(ICashingStrategy):

    def __init__(self):
        ICashingStrategy.__init__(self)

    def deleteLeastRecentDataset(self):
        dataset = min(self.storage.dataset_dict, key=(lambda key: self.storage.dataset_dict[key].timestamp))
        self.storage.rmDataset(dataset.id)

    def clearStorage(self, dataset):
        # Infinit loop guard (already checked in storage when adding dataset)
        if self.storage.storage_byte_size < dataset.byte_size:
            raise Exception("Dataset size is greater than storage size")

        while self.storage.getAvailableSpace() < dataset.byte_size:
            self.deleteLeastRecentDataset()

