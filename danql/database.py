import os
import sqlite3
from contextlib import closing

class Database:
    # Base db class for handling connections and executing sql statements
    # Get db_file from env var, passed in filepath, or fall back to memory
    def __init__(self, db_file=None):
        self.db_file = db_file
        if db_file is not None:
            self.conn = sqlite3.connect(db_file)
        else:
            self.conn = sqlite3.connect(os.getenv('DANQL_DB_FILE', ':memory:'))
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()

    def query(self, sql):
        # Return List[sqlite3.Row] or List[]
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            raise e
        if len(results) > 0:
            return results
        else:
            return []

    def insert(self, sql):
        # Return row_id of created row or None if row already exists
        try:
            self.cur.execute(sql)
            return self.cur.lastrowid
        except sqlite3.IntegrityError as e:
            return None

    def from_sqlfile(self, sqlfile):
        # Return List[sqlite3.Row] or List[]
        try:
            with open(sqlfile, 'r') as f:
                self.cur.executescript(f.read())
            results = self.cur.fetchall()
        except Exception as e:
            raise e
        if len(results) > 0:
            return results
        return []

    def backup(self):
        backup_file = self.db_file + '.bak'
        self.query(f"VACUUM main INTO {backup_file}")

    def create_tables(self, sqlfile, out_directory=None):
        # Create all tables in sqlfile and then create classes from them
        try:
            self.from_sqlfile(sqlfile)
        except Exception as e:
            raise e
        tables = self.query(
            """
            SELECT 
                name
            FROM 
                sqlite_master 
            WHERE 
                type ='table' AND 
                name NOT LIKE 'sqlite_%'
            """)
        for table in tables:
            table = table[0]
            class_definition = self.class_definition_from_table_name(table)
            if out_directory is None:
                print(class_definition)
                return

            out_directory = out_directory.strip('/')
            filepath = f'{out_directory}/{table}.py'
            if os.path.exists(filepath):
                continue
            with open(filepath, 'w') as f:
                f.write(class_definition)
            init_dot_py = f'{out_directory}/__init__.py'
            with open(init_dot_py, 'a') as f:
                f.write(f'from .{table} import {table.capitalize()}\n')

    @staticmethod
    def class_definition_from_table_name(table_name):

        def underscore_to_camelcase(table_name):
            return ''.join(x.capitalize() or '_' for x in table_name.split('_'))

        camel_case = underscore_to_camelcase(table_name)

        template = f"""
# {table_name}.py
from danql import Database, Table

class {camel_case}(Table):
    def __init__(self, db_file=None):
        super().__init__(table_name='{table_name}', db_file=db_file)
"""
        return template.lstrip()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.cur.close()
        if isinstance(exc_value, Exception):
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()
