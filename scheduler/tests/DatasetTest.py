import unittest
from ..src.Dataset import Dataset

class DatasetTest(unittest.TestCase):

    def test_init(self):
        dataset = Dataset("my_dataset", 10)
        self.assertEqual(dataset.id, "my_dataset")
        self.assertEqual(dataset.byte_size, 10)

    def test_cloneAndRefreshTimestamp(self):
        dataset = Dataset("my_dataset", 10)
        new_dataset = dataset.cloneAndRefreshTimestamp()
        # Check that id and byte size are well cloned
        self.assertEqual(dataset.id, new_dataset.id)
        self.assertEqual(dataset.byte_size, new_dataset.byte_size)
        # Check that timestamp is updated
        self.assertGreater(new_dataset.timestamp, dataset.timestamp)

