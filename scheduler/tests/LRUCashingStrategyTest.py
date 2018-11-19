import unittest 
from ..src.Storage import Storage
from ..src.LRUCashingStrategy import LRUCashingStrategy
from ..src.Dataset import Dataset

class LRUCashingStrategyTest(unittest.TestCase):

    def test_init(self):
        c = LRUCashingStrategy()
        self.assertEqual(c.storage, None)
        storage = Storage("storage_1", "storage_name", 20, c)
        self.assertEqual(c.storage, storage)

    def test_clearStorage_nominal_oneDelete(self):
        c = LRUCashingStrategy()
        self.assertEqual(c.storage, None)
        storage = Storage("storage_1", "storage_name", 20, c)
        storage.addDataset(Dataset("1", 15))
        storage.addDataset(Dataset("2", 5))
        storage.addDataset(Dataset("3", 10))  # Clear storage is called by addDataset
        self.assertEqual(storage.used_storage_byte_size, 15)
        self.assertEqual(storage.getDataset("1"), None)
        self.assertNotEqual(storage.getDataset("2"), None)
        self.assertNotEqual(storage.getDataset("3"), None)

    def test_clearStorage_nominal_multipleDelete(self):
        c = LRUCashingStrategy()
        self.assertEqual(c.storage, None)
        storage = Storage("storage_1", "storage_name", 20, c)
        storage.addDataset(Dataset("1", 11))
        storage.addDataset(Dataset("2", 5))
        storage.addDataset(Dataset("3", 17))  # Clear storage is called by addDataset
        self.assertEqual(storage.used_storage_byte_size, 17)
        self.assertEqual(storage.getDataset("1"), None)
        self.assertEqual(storage.getDataset("2"), None)
        self.assertNotEqual(storage.getDataset("3"), None)

    def test_clearStorage_enoughSpace(self):
        c = LRUCashingStrategy()
        self.assertEqual(c.storage, None)
        storage = Storage("storage_1", "storage_name", 20, c)
        storage.addDataset(Dataset("1", 11))
        storage.addDataset(Dataset("2", 5))
        self.assertEqual(storage.used_storage_byte_size, 16)

    def test_clearStorage_datasetToBig(self):
        c = LRUCashingStrategy()
        storage = Storage("storage_1", "storage_name", 20, c)
        ds1 = Dataset("ds_1", 30)
        try:
            c.clearStorage(ds1)
            self.assertFail("Expected to throw an exception")
        except Exception as inst:
            self.assertEqual(inst.message, "Dataset size is greater than storage size")