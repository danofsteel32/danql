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

    def test_batch_insert_one_column_only_violate_constraint(self):
        self.Breed.create_record(name='german shepherd')
        val_list = [dict(name='cow'), dict(name='cat'), dict(name='german shepherd')]
        rows_created = self.Breed.batch_insert(val_list=val_list)
        self.assertEqual(rows_created, 2)

    # .05s slowest test
    def test_batch_insert_multiple_columns_violate_constraint(self):
        gs_id = self.Breed.create_record(name='german shepherd')
        aus_id = self.Breed.create_record(name='aussie shepherd')
        cbf_id = self.Owner.create_record(name='chef bobby flay')
        self.Dog.create_record(breed_id=gs_id, owner_id=cbf_id, name='larry')
        val_list = [
            dict(breed_id=aus_id, owner_id=cbf_id, name='jerry'),
            dict(breed_id=gs_id, owner_id=cbf_id, name='larry')
        ]
        rows_created = self.Dog.batch_insert(val_list)
        self.assertEqual(rows_created, 1)

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

    def test_get_id_when_no_id(self):
        corn_dog = self.Breed.get_id(name='corn dog')
        self.assertIsNone(corn_dog)

    def test_get_id(self):
        self.Breed.create_record(name='german shepherd')
        gs_id = self.Breed.get_id(name='german shepherd')
        self.assertEqual(gs_id, 1)

    def test_row_set_intersection(self):
        gs_id = self.Breed.create_record(name='german shepherd')
        aus_id = self.Breed.create_record(name='aussie shepherd')
        cbf_id = self.Owner.create_record(name='chef bobby flay')
        bob_id = self.Owner.create_record(name='bob bobbers')
        dog_list = [
            dict(breed_id=aus_id, owner_id=cbf_id, name='jerry'),
            dict(breed_id=gs_id, owner_id=cbf_id, name='garry'),
            dict(breed_id=aus_id, owner_id=cbf_id, name='larry'),
            dict(breed_id=gs_id, owner_id=bob_id, name='bob_dog')
        ]
        self.Dog.batch_insert(dog_list)
        # Now get all dogs where breed=gs and owner!=cbf
        gs_breed_dogs = self.Dog.read_record(breed_id=gs_id)
        not_cbf = self.Dog.read_record(owner_id=cbf_id, not_equal=True)

        intersection = gs_breed_dogs.intersection(not_cbf)
        self.assertEqual((4, 1, 2, 'bob_dog'), tuple(intersection.pop()))

    def test_sql_injection(self):
        gs_id = self.Breed.create_record(name='german shepherd')
        sql_injection = self.Breed.create_record(name='DROP TABLE breed;')
        self.Breed.read_record(name='german shepherd')

    def test_raw_query_from_sqlfile(self):
        self.Breed.create_record(name='german shepherd')
        results = self.Breed.raw_query(sqlfile='tests/sql/get_breeds.sql')

if __name__ == '__main__':
    unittest.main()
