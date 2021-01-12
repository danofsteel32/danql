import os
from danql import Database

Database('/home/dan/danql/tests/test.db').create_tables(sqlfile='./tests/test_tables.sql', out_directory='out/')
