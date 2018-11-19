class Storage:

    def __init__(self, id, name, storage_byte_size, cashing_strategy):
        self.id = id
        self.name = name
        self.storage_byte_size = storage_byte_size
        self.used_storage_byte_size = 0
        self.dataset_dict = dict()
        self.cashing_strategy = cashing_strategy
        if self.cashing_strategy is not None:
            cashing_strategy.setStorage(self)

    def getAvailableSpace(self):
        return self.storage_byte_size - self.used_storage_byte_size
    
    def rmDataset(self, id):
        dataset = self.dataset_dict.pop(id, None)
        # Update storage size if dataset was removed
        if dataset is not None:
            new_byte_size = self.used_storage_byte_size - dataset.byte_size
            self.used_storage_byte_size = new_byte_size

    def addDataset(self, dataset):
        # Check that dataset is contained, if so, update timestamp
        if self.getDataset(dataset.id) is not None:
            self.dataset_dict[dataset.id] = dataset.cloneAndRefreshTimestamp()
            return

        # Check that the dataset can be added 
        if self.storage_byte_size < dataset.byte_size:
            raise Exception("Dataset size is greater than storage size")

        # Apply cashing strategy if storage is full
        if self.getAvailableSpace() < dataset.byte_size:
            if self.cashing_strategy is None:
                raise Exception("Storage need clean and no cashing strategy is set")
            self.cashing_strategy.clearStorage(dataset)
        
        # Add dataset
        new_byte_size = self.used_storage_byte_size + dataset.byte_size
        self.dataset_dict[dataset.id] = dataset.cloneAndRefreshTimestamp()
        self.used_storage_byte_size = new_byte_size

    def getDataset(self, id_dataset):
        if id_dataset in self.dataset_dict:
            return self.dataset_dict[id_dataset]
        else:
            return None