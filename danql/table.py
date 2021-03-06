import logging
import os
import itertools

from .database import Database

if os.getenv('DEBUG', None) is not None:
    logging.basicConfig(format='%(message)s', level=logging.DEBUG)

class Table:
    """ Abstract database table class

    All attributes besides db_file and table_name are properties set on init
    by querying the sqlite database with PRAGMA statements.
    See https://www.sqlite.org/pragma.html for PRAGMA docs.

    Attributes
    ----------
    db_file : str
        sqlite database filepath
    table_name : str
        name of table in sqlite database
    columns : dict
        keys are column names, values are Column class
    indexes : set
        set of column names that are indexes or have unique constraint
    primary_keys : List[str]
        list of column names that are primary_keys
    parents : List[dict]
        list of parent tables and column relationships
    foreign_keys: set
        set of column names that are foreign_keys

    Methods
    -------
    create_record(**kwargs):
        inserts a single row
    read_record(not_equal=False, **kwargs):
        gets all rows in table constructing WHERE clause from kwargs
    update_record(rows, not_equal=False, **kwargs):
        updates every row in rows to values in kwargs
    delete_record(rows):
        delete every row in rows
    check_column_args(column_args):
        raises ValueError if column arguments are not column names on table
    sanitize_kwargs(**kwargs):
        transforms kwargs into sanitized lists of columns and values
    primary_keys_from_rows(rows)
        used by update_record and delete_record
    total_rows():
        get total number of rows in table
    count_where(not_equal=False, **kwargs):
        gets count of rows constructing WHERE clause from kwargs
    sqlfile_query(sqlfile):
        load queries from a sqlfile too complex for basic CRUD methods
    raw_query(sql):
        execute arbitrary sql statement
    column_equal_value(col_val_pairs, not_equal=False):
        helper function for constructing WHERE clauses
    properly_quoted(values):
        helper function for sanitizing values before making queries
    """

    def __init__(self, table_name, columns={}, db_file=None, indexes=set(),
                 primary_keys=[], foreign_keys=set(), parents=[]):

        self.db_file = db_file
        self.table_name = table_name
        self.columns = columns
        self.indexes = indexes
        self.primary_keys = primary_keys
        self.parents = parents
        self.foreign_keys = foreign_keys

    @property
    def columns(self):
        return self.__columns
    @columns.setter
    def columns(self, columns):
        if len(columns.keys()) > 0:
            self.__columns = self.columns
        else:
            sql = "PRAGMA table_info(%s)" % (self.table_name,)
            with Database(self.db_file) as db:
                schema = db.query(sql)
            # TODO better var name than to_set
            to_set = {}
            for col in schema:
                name = col['name']
                nullable = False if col['notnull'] else True
                pk = True if col['pk'] else False
                column = Column(
                    name=name,
                    type=col['type'],
                    nullable=nullable,
                    default_value=col['dflt_value'],
                    primary_key=pk
                )
                to_set[name] = column
            self.__columns = to_set

    # Pretty sure indexes is a standard object method maybe want to rename this
    @property
    def indexes(self):
        return self.__indexes
    @indexes.setter
    def indexes(self, indexes):
        if len(indexes) > 0:
            self.__indexes = indexes
        else:
            with Database(self.db_file) as db:
                index_info = [
                    db.query(f"PRAGMA index_info({i['name']})")
                    for i in db.query(f"PRAGMA index_list({self.table_name})")
                ]
            indexed_columns = [
                idx['name'] for row in index_info
                for idx in row
                if len(index_info) > 0
            ]
            self.__indexes = set(indexed_columns)

    @property
    def primary_keys(self):
        return self.__primary_keys
    @primary_keys.setter
    def primary_keys(self, primary_keys):
        if len(primary_keys) > 0:
            self.__primary_keys = primary_keys
        else:
            pks = [col for col in self.columns if self.columns[col].primary_key]
            self.__primary_keys= pks

    @property
    def parents(self):
        return self.__parents
    @parents.setter
    def parents(self, parents):
        if len(parents) > 0:
            self.__parents = parents
        else:
            with Database(self.db_file) as db:
                fks = db.query(f"PRAGMA foreign_key_list({self.table_name})")
            if len(fks) > 0:
                self.__parents = [dict(fk) for fk in fks]
            else:
                self.__parents = []

    @property
    def foreign_keys(self):
        return self.__foreign_keys
    @foreign_keys.setter
    def foreign_keys(self, foreign_keys):
        if len(foreign_keys) > 0:
            self.__foreign_keys = foreign_keys
        else:
            self.__foreign_keys = set([parent['from'] for parent in self.parents])

    def create_record(self, **kwargs):
        # Return newly created row_id/pk
        # or return existing row_id/pk of those values
        # if inserting those values raises
        # a sqlite3.IntegrityError (violated unique constraint)
        columns, values = self.sanitize_kwargs(**kwargs)
        columns = ','.join(columns)
        values = self.properly_quoted(values)
        sql = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
        logging.debug(sql)
        with Database(self.db_file) as db:
            new_row_id = db.insert(sql)
        if new_row_id is not None:
            return new_row_id

        logging.debug('Row already exists')
        existing_row = self.read_record(**kwargs)[0]
        existing_row_id = [existing_row[x] for x in self.primary_keys]
        if len(existing_row_id) > 1:
            return tuple(existing_row_id)
        elif len(existing_row_id) == 1:
            return existing_row_id[0]

    def read_record(self, not_equal=False, **kwargs):
        # return List[rows] or empty List if no rows
        columns, values = self.sanitize_kwargs(**kwargs)
        repacked = dict(zip(columns, values))
        col_val_pairs = self.column_equal_value(repacked, not_equal=not_equal)
        sql = f"SELECT * FROM {self.table_name} WHERE {col_val_pairs}"
        logging.debug(sql)
        with Database(self.db_file) as db:
            results = db.query(sql)
        return results

    def update_record(self, rows, not_equal=False, **kwargs):
        # rows is set of rows to be updated
        # kwargs is col=val; col gets updated to val on $rows
        # returns list of updated rows
        if rows is None:
            raise ValueError("rows is required argument")

        self.check_column_args(kwargs.keys())
        col_val_pairs = self.column_equal_value(kwargs, not_equal=not_equal)
        update_statements = [
            (f"UPDATE {self.table_name} " 
             f"SET {col_val_pairs} "
             f"WHERE {pk}")
            for pk in self.primary_keys_from_rows(rows)
        ]
        with Database(self.db_file) as db:
            for statement in update_statements:
                logging.debug(statement)
                db.query(statement)
        with Database(self.db_file) as db:
            sql = f"SELECT * FROM {self.table_name} WHERE {col_val_pairs}"
            logging.debug(sql)
            updated_rows = db.query(sql)
        return updated_rows

    def delete_record(self, rows):
        # rows is set of rows to be deleted
        # Returns number of rows deleted
        if rows is None:
            raise ValueError("rows is required argument")

        delete_statements = [
            f"DELETE FROM {self.table_name} WHERE {pk}"
            for pk in self.primary_keys_from_rows(rows)
        ]
        before_count = self.total_rows()
        with Database(self.db_file) as db:
            for statement in delete_statements:
                logging.debug(statement)
                db.query(statement)
        after_count = self.total_rows()
        row_delta = before_count - after_count
        return row_delta

    def check_column_args(self, column_args):
        for col in column_args:
            if col not in self.columns:
                error_msg = f'{col} is not a valid column on {self.table_name}'
                raise ValueError(error_msg)
        return True

    def sanitize_kwargs(self, **kwargs):
        columns = list(kwargs.keys())
        if self.check_column_args(columns):
            values = list(kwargs.values())
            for n, v in enumerate(values):
                if v is None:
                    del columns[n]
                    del values[n]
            return columns, values

    def primary_keys_from_rows(self, rows):
        pk_columns = [
            column
            for column in rows[0].keys()
            if column in self.primary_keys
        ]
        pks = [row[column] for row in rows for column in pk_columns]
        y = itertools.product(pk_columns, pks)
        return [self.column_equal_value({z[0]: z[1]}) for z in y]

    def total_rows(self):
        # Count of every row in tables
        with Database(self.db_file) as db:
            count = db.query(f"SELECT count(*) FROM {self.table_name}")
        return count.pop()['count(*)']

    def count_where(self, not_equal=False, **kwargs):
        # when not_equal: WHERE col!=val instead of col=val
        if self.check_column_args(kwargs.keys()):
            col_val_pairs = self.column_equal_value(kwargs, not_equal=not_equal)
        sql = f"SELECT count(*) FROM {self.table_name} WHERE {col_val_pairs}"
        logging.debug(sql)
        with Database(self.db_file) as db:
            count = db.query(sql)
        return count.pop()['count(*)']

    def sqlfile_query(self, sqlfile):
        # Load query from a sqlfile
        with Database(self.db_file) as db:
            results = db.from_sqlfile(sqlfile)
        return results

    def raw_query(self, sql):
        with Database(self.db_file) as db:
            results = db.query(sql)
        return results

    def column_equal_value(self, col_val_pairs, not_equal=False):
        pairs = []
        for column in col_val_pairs:
            value = self.properly_quoted(col_val_pairs[column])
            if not_equal:
                pairs.append(f"{column}!={value}")
            else:
                pairs.append(f"{column}={value}")
        return ' AND '.join(pairs)

    @staticmethod
    def properly_quoted(values):
        def quoted(val):
            if type(val) == str:
                val = val.replace('"', '')
                val = val.replace ("'", '')
                return f"'{val}'"
            elif type(val) == int or type(val) == float:
                return str(val)
            elif type(val) == bool:
                if val:
                    return '1'
                return '0'
            else:
                raise ValueError(f"Unsupported type {str(type(val))}")
        if type(values) == list:
            return ','.join(quoted(value) for value in values)
        return quoted(values)

    def __call__(self):
        return self

    def __str__(self):
        # This is pretty cancerous but its string building what do you expect
        # Only needed to pretty print table schema
        cols = ['  {0}\n'.format(col.__str__()) for col in self.columns.values()]
        c = ''.join(cols)
        parents = ['  Parent(table={0} from={1} to={2})\n'.format(p['table'], p['from'], p['to']) for p in self.parents]
        p = ''.join(parents)
        pk = '  PrimaryKey({0})\n'.format(', '.join([p for p in self.primary_keys]))
        if len(self.foreign_keys) == 0:
            fk = ''
        else:
            fk = '  ForeignKey({0})\n'.format(', '.join([f for f in self.foreign_keys]))
        if len(self.indexes) == 0:
            idx = ''
        else:
            idx = '  Index({0})\n'.format(', '.join([i for i in self.indexes]))
        return (f'Table {self.table_name} \n{c}{pk}{idx}{p}{fk}')

class Column:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', None)
        self.type = kwargs.get('type', None)
        self.nullable = kwargs.get('nullable', None)
        self.default_value = kwargs.get('default_value', None)
        self.primary_key = kwargs.get('primary_key', None) # Bool

    def __str__(self):
        return (f'Column(name={self.name} '
                f'type={self.type} '
                f'nullable={self.nullable} '
                f'default_value={self.default_value})')
