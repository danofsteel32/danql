# danql

## Disclaimer
Not meant to actually be used by anyone. This is bad code because it uses string building to create queries. Wrote this to learn how an ORM might be implemented and
see first hand the
[object-relational impedence mismatch](http://blogs.tedneward.com/post/the-vietnam-of-computer-science/).


### Running Tests
```
make test
```
### Installation
```
make install
```
### Usage
Define your table(s) in a regular sql file. I usually just call it `tables.sql`.
``` sql
CREATE TABLE IF NOT EXISTS example (
    id           INTEGER PRIMARY KEY,
    example_text TEXT NOT NULL
);
```
Note that you can set the database file in your code
or set it through an environment variable.
``` sh
export DANQL_DB_FILE=<path to your db file here>
```
Now in your python module
``` python
from danql import Database

# database file set in env var is automatically picked up
Database().create_tables(sqlfile='tables.sql')

# or set database file in module
db_file = 'example.db'
Database(db_file=db_file).create_tables(sqlfile='tables.sql')
```
This will create your table(s) and create starter classes for each one of your
tables. For our example table it would look like this
``` python
# example.py
from danql import Database, Table

class Example(Table):
    def __init__(self, db_file=None):
        super().__init__(table_name='example', db_file=db_file)
```
By default the starter classes will be printed on stdout, but you can also specify
a directory where each class will be written out in its own file. The directory must
exist before calling `create_tables()`.
``` python
Database().create_tables(sqlfile='tables.sql', out_directory='db')
```
Would give you `db/example.py` which you can then use in your code like
``` python
from db.example import Example

db_file = 'example.db'
example_table = Example(db_file=db_file)
example_text_id = example_table.create_record(example_text='go big or go home')
```

### Design Notes
Inserts are idempotent. If the values you are trying to insert 
are would violate a unique constraint then the`sqlite3.IntegrityError` exception is handled gracefully and the primary key associated with those values is returned.

Updates and deletes require that you first select the rows you want to delete
and then pass those rows as an argument to the update and delete methods.
