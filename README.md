# danql

## Disclaimer
Only works for SQLite and relies on string building to generate queries so not
safe for web. Not meant for production use. Simply my attempt to
deal with the
[object-relational impedence mismatch](http://blogs.tedneward.com/post/the-vietnam-of-computer-science/).

## About
Orms typically don't work very well for people who like SQL. But writing the basic 
CRUD ops for every table simply takes too long. This library is the sweet spot for me 
in that it makes the easy stuff quick and the hard stuff easily doable in plain sql.

## Usage Guide
You probably already have SQLite installed but if you don't just install it
with your distros package manager or brew.

### Running Tests
```
make test
```
### Installation
``` sh
pip install -e .
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
Inserts are idempotent. If the values you are trying to insert are already
associated with a primary key or would violate a unique constraint then the
`sqlite3.IntegrityError` is handled gracefully and the primary key associated
with those values is returned

Updates and deletes require that you first select the rows you want to delete
and then pass those rows as an argument to the update and delete methods.

## Goals
* No dependencies outside of SQLite
* Small, consistent, low complexity codebase

## Non-Goals
* Competing with sqlalchemy or any other ORM
* Supporting any database besides SQLite

## TODO
* mypy and type hinting
* primary keys can be named something else besides id in get\_id()
* build more complex example
* docstrings
* Pretty up code
* consolidate columns,values parsing into 1 function build\_query\_or\_fail
