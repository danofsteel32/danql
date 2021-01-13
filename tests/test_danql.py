import glob
import os
import sys
import unittest
import shutil


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from danql import Database

class TestDanql(unittest.TestCase):
    def setUp(self):
        self.db_file = 'tests/test.db'
        self.out_dir = 'tests/out'
        os.mkdir(self.out_dir)
        Database(db_file=self.db_file).create_tables(sqlfile='tests/test_tables.sql', out_directory=self.out_dir)
        from out import breed, dog, owner
        self.Breed = breed.Breed(self.db_file)
        self.Dog = dog.Dog(self.db_file)
        self.Owner = owner.Owner(self.db_file)

    def tearDown(self):
        os.remove(self.db_file)
        shutil.rmtree(self.out_dir)

    def test_all_properties_on_table_class(self):
        print(self.Breed)
        print(self.Dog)
        print(self.Owner)

    def test_inserting_same_values_violation(self):
        gs_id1 = self.Breed.create_record(name='german shepard')
        gs_id2 = self.Breed.create_record(name='german shepard')
        self.assertEqual(gs_id1, gs_id2)

if __name__ == '__main__':
    unittest.main()
    print('TESTS PASSED')
