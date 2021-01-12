from .database import Database

class Table:
    """ Abstraction of a database table
    """
    def __init__(self, table_name, columns={}, indexes=set(), parents=[], is_child=False, 
                 primary_keys=set(), foreign_keys=set(), db_file=None):
        self.db_file = db_file
        self.table_name = table_name
        self.columns = columns # dict where keys are column names
        self.indexes = indexes
        self.primary_keys = primary_keys
        self.parents = parents # parent_table, from_column, to_column
        self.is_child = is_child
        self.foreign_keys = foreign_keys

    @property
    def columns(self):
        return self.__columns
    @columns.setter
    def columns(self, columns):
        if len(columns.keys()) > 0:
            return self.__columns

        sql = "PRAGMA table_info(%s)" % (self.table_name,)
        with Database(self.db_file) as db:
            schema = db.query(sql)
        # TODO better var name than to_set
        to_set = {}
        for col in schema:
            name = col['name']
            nullable = False if col['notnull'] else True
            primary_key = True if col['pk'] else False
            column = Column(name=name,
                            type=col['type'],
                            nullable=nullable,
                            default_value=col['dflt_value'],
                            primary_key=primary_key)
            to_set[name] = column
        self.__columns = to_set

    # Pretty sure indexes is a standard object method maybe want to rename this
    @property
    def indexes(self):
        return self.__indexes
    @indexes.setter
    def indexes(self, indexes):
        if len(indexes) > 0:
            return self.__indexes

        indexes = set()
        with Database(self.db_file) as db:
            index_list = db.query("PRAGMA index_list(%s)" % (self.table_name,))
            if index_list is not None:
                for index in index_list:
                    # TODO separate unique and non-unique indexes
                    index_info = db.query("PRAGMA index_info(%s)" % (index['name']))
                    for i in index_info:
                        indexes.add(i['name'])
        self.__indexes = indexes

    @property
    def primary_keys(self):
        return self.__primary_keys
    @primary_keys.setter
    def primary_keys(self, primary_keys):
        if len(primary_keys) > 0:
            return self.__primary_keys
        pks = set()
        for col in self.columns:
            if self.columns[col].primary_key:
                pks.add(col)
        self.__primary_keys = pks

    @property
    def parents(self):
        return self.__parents
    @parents.setter
    def parents(self, parents):
        if len(parents) > 0:
            return self.__parents
        with Database(self.db_file) as db:
            fks = db.query("PRAGMA foreign_key_list(%s)" % (self.table_name,))
        parents_list = []
        if fks is not None:
            for fk in fks:
                p = {'table': fk['table'], 'from': fk['from'], 'to': fk['to']}
                parents_list.append(p)
        self.__parents = parents_list

    @property
    def foreign_keys(self):
        return self.__foreign_keys
    @foreign_keys.setter
    # Nice to have quick reference on what cols are fks
    # Don't have to unpack columns to access
    def foreign_keys(self, foreign_keys):
        if len(foreign_keys) > 0:
            return self.__foreign_keys
        self.__foreign_keys = set([parent['from'] for parent in self.parents])

    @property
    def is_child(self):
        return self.__is_child
    @is_child.setter
    def is_child(self, is_child):
        # If we have parent tables we are a child table
        # TODO figure out some way to have a set of children tables like in parents
        if is_child:
            return self.__is_child
        if len(self.parents) > 0:
            self.__is_child = True

    def check_column_args(self, column_args):
        columns = self.columns.keys()
        for col in column_args:
            if col not in self.columns:
                print(self.columns)
                error_msg = 'Column {0} is not a valid column on table {1}'
                raise ValueError(error_msg.format(col, self.table_name))
        return True

    def sanitize_kwargs(self, **kwargs):
        # No None
        columns = list(kwargs.keys())
        if self.check_column_args(columns):
            values = list(kwargs.values())
            for n, v in enumerate(values):
                if v is None:
                    del columns[n]
                    del values[n]
            return columns, values

    def create_record(self, **kwargs):
        # Return newly created row id (pk) or return 
        # existing row id if inserting those values raises
        # a sqlite3.IntegrityError (violated unique contrainst)
        columns, values = self.sanitize_kwargs(**kwargs)
        columns = ','.join(columns)
        values = self.properly_quoted(values)
        sql = "INSERT INTO {0} ({1}) VALUES ({2})"
        sql = sql.format(self.table_name, columns, values)
        print(sql)
        with Database(self.db_file) as db:
            new_row_id = db.insert(sql)
            if new_row_id is None:
                print('Row already exists')
                columns, values = self.sanitize_kwargs(**kwargs)
                col_val_zipped = dict(zip(columns, values))
                existing_row_id = self.get_id(**col_val_zipped)
                if existing_row_id is None:
                    # 99% sure This should never happen but
                    raise Exception('WTF create_record')
                return existing_row_id
            return new_row_id

    def read_record(self, not_equal=False, **kwargs):
        # return List
        columns, values = self.sanitize_kwargs(**kwargs)
        col_val_pairs = self.column_equal_value(dict(zip(columns, values)))
        sql = "SELECT * FROM {0} WHERE {1}"
        sql = sql.format(self.table_name, col_val_pairs)
        print(sql)
        with Database(self.db_file) as db:
            results = db.query(sql)
        return results


    def update_record(self, rows, not_equal=False, **kwargs):
        # rows is list of rows to be updated
        # kwargs is col=val; col gets updated to val on $rows
        # returns list of updated rows
        if rows is None:
            print('rows was None')
            return None

        if self.check_column_args(kwargs.keys()):
            col_val_pairs = self.column_equal_value(kwargs, not_equal=not_equal)
            update_statements = []
            select_statements = []
            for row in rows:
                for col in row.keys():
                    if col in self.primary_keys:
                        update_sql = "UPDATE {0} SET {1} WHERE {2}"
                        where = self.column_equal_value({col: row[col]})
                        update_sql = update_sql.format(self.table_name, col_val_pairs, where)
                        select_sql = "SELECT * FROM {0} WHERE {1}"
                        select_sql = select_sql.format(self.table_name, col_val_pairs)
                        update_statements.append(update_sql)
                        select_statements.append(select_sql)
    
            updated_rows = []
            with Database(self.db_file) as db:
                for statement in update_statements:
                    print(statement)
                    db.query(statement)
                for statement in select_statements:
                    print(statement)
                    updated_rows.append(db.query(statement)[0])
            return updated_rows

    def delete_record(self, rows):
        # rows is list of rows to be deleted
        # Returns number of rows deleted
        if rows is None:
            print('rows was None')
            return None
        delete_statements = []
        for row in rows:
            for col in row.keys():
                if col in self.primary_keys:
                    set_ = self.column_equal_value({col: row[col]})
                    sql = "DELETE FROM {0} WHERE {1}"
                    sql = sql.format(self.table_name, set_)
                    delete_statements.append(sql)
        before_count = self.total_rows()
        with Database(self.db_file) as db:
            for statement in delete_statements:
                print(statement)
                db.query(sql)
        after_count = self.total_rows()
        row_delta = before_count - after_count
        return row_delta

    def batch_insert(self, val_list):
        # val_list is [dicts]
        # Much faster when inserting lots of rows
        # Returns number of rows created
        # TODO CLEANUP
        columns = val_list[0].keys()
        if self.check_column_args(columns):
            columns = ','.join(list(columns))
            val_list = [tuple(v.values()) for v in val_list]
            vals = '{0}'.format(','.join(str(v) for v in val_list))
            sql = "INSERT INTO {0} ({1}) VALUES {2}".format(self.table_name, columns, vals)
            print(sql)
            before_count = self.total_rows()
            with Database(self.db_file) as db:
                db.insert(sql)
            after_count = self.total_rows()
            row_delta = after_count - before_count
            return row_delta

    def total_rows(self):
        # Count of every row in tables
        with Database(self.db_file) as db:
            count = db.query("SELECT count(*) FROM {0}".format(self.table_name))
        return count[0][0]

    def count_where(self, not_equal=False, **kwargs):
        # when not_equal: WHERE col!=val instead of col=val
        if self.check_column_args(kwargs.keys()):
            col_val_pairs = self.column_equal_value(kwargs, not_equal=not_equal)
        sql = "SELECT count(*) FROM {0} WHERE {1}".format(self.table_name, col_val_pairs)
        print(sql)
        with Database(self.db_file) as db:
            count = db.query(sql)
        return count[0][0]

    def raw_query(self, sqlfile):
        # Load query from a sqlfile
        with Database(self.db_file) as db:
            results = db.from_sqlfile(sqlfile)
        return results

    def get_id(self, **kwargs):
        # Just want the id of some row(s)
        if self.check_column_args(kwargs.keys()):
            col_val_pairs = self.column_equal_value(kwargs)

            sql = "SELECT id FROM {0} WHERE {1}"
            sql = sql.format(self.table_name, col_val_pairs)
            print(sql)
            with Database(self.db_file) as db:
                results = db.query(sql)
                if results is not None:
                    if len(results) == 1:
                        return results[0]['id']
                return results
    
    def column_equal_value(self, col_val_pairs, not_equal=False):
        pairs = []
        for column in col_val_pairs:
            if not_equal:
                pairs.append('{0}!={1}'.format(column, self.properly_quoted(col_val_pairs[column])))
            else:
                pairs.append('{0}={1}'.format(column, self.properly_quoted(col_val_pairs[column])))
        return ' AND '.join(pairs)

    @staticmethod
    def properly_quoted(values):
        def quoted(val):
            if type(val) == str:
                return "'{0}'".format(val)
            elif type(val) == int or type(val) == float:
                return str(val)
            elif type(val) == bool:
                if val:
                    return '1'
                return '0'
            else:
                raise ValueError('ERROR: unsupported type %s' % str(type(val)))
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
