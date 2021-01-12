import glob
import os
import sys
from collections import namedtuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from danql import Database

def dict_to_nametuple(dict_arg):
    Row = namedtuple('Row', dict_arg.keys())
    r = Row(**dict_arg)
    return r

db_file = 'tests/test.db'
out_dir = 'tests/out'
if os.path.exists(db_file):
    os.remove(db_file)
if os.path.exists(out_dir):
    files = glob.glob(out_dir + '/*')
    for f in files:
        try:
            os.remove(f)
        except IsADirectoryError:
            pass
else:
    os.mkdir('tests/out')
Database(db_file=db_file).create_tables(sqlfile='tests/test_tables.sql', out_directory=out_dir)

from out import breed, dog, owner

# MAKE SURE TABLE CLASS IS WORKING FOR ALL TABLES
print(db_file)
print(breed.Breed(db_file=db_file))
print(dog.Dog(db_file=db_file))
print(owner.Owner(db_file=db_file))

# Try insert twice
gs_id = breed.Breed(db_file=db_file).create_record(name='german shepard')
gs_id = breed.Breed(db_file=db_file).create_record(name='german shepard')
print('german shepard id = %d' % gs_id)

bob = owner.Owner(db_file=db_file).create_record(name='bob barker')
zo_id = dog.Dog(db_file=db_file).create_record(name='zo', breed_id=gs_id, owner_id=bob)
print('Dog %d created' % zo_id)

# Fail unique contrainst
new_zo = dog.Dog(db_file=db_file).create_record(name='zo', breed_id=gs_id, owner_id=bob)

zo = dog.Dog(db_file=db_file).read_record(id=zo_id)
print([dict_to_nametuple(r) for r in zo])

now_bro = dog.Dog(db_file=db_file).update_record(zo, name='bro')
print([dict_to_nametuple(r) for r in now_bro])

new_dogs = [dict(name='fido', breed_id=gs_id), dict(name='leroy', breed_id=gs_id)]
inserted = dog.Dog(db_file=db_file).batch_insert(new_dogs)
print('Rows created = %d' % inserted)

not_bro = dog.Dog(db_file=db_file).count_where(name='bro', not_equal=True)
print('Dogs who are not bro = %d' % not_bro)

bro_pre_delete = dog.Dog(db_file=db_file).count_where(name='bro')
print('Before delete %d' % bro_pre_delete)

delete_bro = dog.Dog(db_file=db_file).delete_record(zo)
print('Rows deleted %d' % delete_bro)

zo_post_delete = dog.Dog(db_file=db_file).count_where(name='bro')
print('Rows after delete %d' % zo_post_delete)

print()
print('TESTS PASSED')
