import unittest
from ..src.Storage import Storage
from ..src.Dataset import Dataset

class StorageTest(unittest.TestCase):

    def test_init(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        self.assertEqual(storage.id, "storage_1")
        self.assertEqual(storage.name, "storage_name")
        self.assertEqual(storage.storage_byte_size, 20)
        self.assertEqual(storage.used_storage_byte_size, 0)
    
    def test_getAvailableSpace(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        storage.used_storage_byte_size = 3
        self.assertEqual(storage.getAvailableSpace(), 17)

    def test_addDataset_nominal(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        storage.addDataset(Dataset("ds_1", 5))
        self.assertEqual(storage.getAvailableSpace(), 15)
        storage.addDataset(Dataset("ds_2", 10))
        self.assertEqual(storage.getAvailableSpace(), 5)

    def test_addDataset_sameName(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 5)
        timestampA = ds1.timestamp
        storage.addDataset(ds1)
        self.assertEqual(storage.getAvailableSpace(), 15)
        timestampB = storage.dataset_dict["ds_1"].timestamp
        self.assertGreater(timestampB, timestampA)
        # User try to add a storage that is already contained
        storage.addDataset(ds1)
        # Check that storage has the same byte size
        self.assertEqual(storage.getAvailableSpace(), 15)
        # Check that timestamp of dataset is updated
        timestampA = storage.dataset_dict["ds_1"].timestamp
        self.assertGreater(timestampA, timestampB)

    def test_addDataset_storageNotBigEnough(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 30)
        try:
            storage.addDataset(ds1)
            self.assertFail("Expected to throw an exception")
        except Exception as inst:
            self.assertEqual(inst.message, "Dataset size is greater than storage size")

    def test_addDataset_needCleanAndNoCashingStrategyUsed(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 15)
        storage.addDataset(ds1)
        ds2 = Dataset("ds_2", 6)
        try:
            storage.addDataset(ds2)
            self.assertFail("Expected to throw an exception")
        except Exception as inst:
            self.assertEqual(inst.message, "Storage need clean and no cashing strategy is set")

    def test_rmDataset_datasetExists(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 10)
        storage.addDataset(ds1)
        self.assertEqual(storage.getAvailableSpace(), 10)
        storage.rmDataset("ds_1")
        self.assertEqual(storage.getAvailableSpace(), 20)

    def test_rmDataset_datasetDoesNotExists(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 10)
        storage.addDataset(ds1)
        self.assertEqual(storage.getAvailableSpace(), 10)
        storage.rmDataset("ds_2")
        self.assertEqual(storage.getAvailableSpace(), 10)

    def test_getDataset_datasetExists(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 10)
        storage.addDataset(ds1)
        self.assertNotEqual(storage.getDataset("ds_1"), None)

    def test_getDataset_datasetDoesNotExists(self):
        storage = Storage("storage_1", "storage_name", 20, None)
        ds1 = Dataset("ds_1", 10)
        storage.addDataset(ds1)
        self.assertEqual(storage.getDataset("ds_2"), None)

