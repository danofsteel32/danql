import glob
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from danql import Database

class TestDanql(unittest.TestCase):
    db_file = 'tests/test.db'
    out_dir = 'tests/out'
    os.mkdir(out_dir)
    Database(db_file=db_file).create_tables(sqlfile='tests/test_tables.sql', out_directory=out_dir)
    from out import breed, dog, owner
    Breed = breed.Breed(db_file)
    Dog = dog.Dog(db_file)
    Owner = owner.Owner(db_file)

    #def setUp(self):

    def tearDown(self):
        with Database(self.db_file) as db:
            db.query("DELETE FROM dog")
            db.query("DELETE FROM owner")
            db.query("DELETE FROM breed")

    def test_all_properties_on_table_class(self):
        print(self.Breed)
        print(self.Dog)
        print(self.Owner)

    def test_inserting_same_values_violation(self):
        gs_id1 = self.Breed.create_record(name='german shepherd')
        gs_id2 = self.Breed.create_record(name='german shepherd')
        self.assertEqual(gs_id1, gs_id2)

    def test_total_rows(self):
        before = self.Breed.total_rows()
        self.assertEqual(before, 0)
        self.Breed.create_record(name='german shepherd')
        after = self.Breed.total_rows()
        self.assertEqual(after, before+1)

    def test_column_equal_value(self):
        col_val_pairs = dict(name='larry', owner_id=1)
        _is = self.Dog.column_equal_value(col_val_pairs)
        _not = self.Dog.column_equal_value(col_val_pairs, not_equal=True)

    def test_update_record(self):
        self.Breed.create_record(name='german shepherd')
        rows = self.Breed.read_record(name='german shepherd')
        new_rows = self.Breed.update_record(rows=rows, name='austrian shepherd')
        self.assertEqual(len(new_rows), 1)

    def test_delete_record(self):
        self.Breed.create_record(name='german shepherd')
        rows = self.Breed.read_record(name='german shepherd')
        deleted = self.Breed.delete_record(rows=rows)
        self.assertEqual(deleted, 1)

    def test_sql_injection(self):
        gs_id = self.Breed.create_record(name='german shepherd')
        sql_injection = self.Breed.create_record(name='DROP TABLE breed;')
        self.Breed.read_record(name='german shepherd')

    def test_sqlfile_query(self):
        self.Breed.create_record(name='german shepherd')
        results = self.Breed.sqlfile_query(sqlfile='tests/sql/get_breeds.sql')

    def test_raw_query(self):
        self.Breed.raw_query("SELECT * FROM breed")

if __name__ == '__main__':
    unittest.main()
