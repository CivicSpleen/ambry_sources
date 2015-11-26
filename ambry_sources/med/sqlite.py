# -*- coding: utf-8 -*-
from datetime import datetime, date

from six import binary_type, text_type

# Documents used to implement module and function:
# Module: http://apidoc.apsw.googlecode.com/hg/vtable.html
# Functions: http://www.drdobbs.com/database/query-anything-with-sqlite/202802959?pgno=3

# python type to sqlite type map.
TYPE_MAP = {
    'int': 'INTEGER',
    'float': 'REAL',
    binary_type.__name__: 'TEXT',
    text_type.__name__: 'TEXT',
    'date': 'DATE',
    'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
}


class Table:
    """ Represents a table """
    def __init__(self, columns, mprows):
        """

        Args:
            columns (list of str): column names
            mprows (mpf.MPRowsFile):

        """
        self.columns = columns
        self.mprows = mprows

    def BestIndex(self, *args):
        return None

    def Open(self):
        return Cursor(self)

    def Disconnect(self):
        pass

    Destroy = Disconnect


class Cursor:
    """ Represents a cursor """
    def __init__(self, table):
        self.table = table
        self._reader = table.mprows.reader
        self._rows_iter = iter(self._reader.rows)
        self._current_row = next(self._rows_iter)
        self._row_id = 1

    def Filter(self, *args):
        pass

    def Eof(self):
        return self._current_row is None

    def Rowid(self):
        return self._row_id

    def Column(self, col):
        value = self._current_row[col]
        if isinstance(value, (date, datetime)):
            # Convert to ISO format.
            return value.isoformat()
        return value

    def Next(self):
        try:
            self._current_row = next(self._rows_iter)
            self._row_id += 1
            assert isinstance(self._current_row, (tuple, list)), self._current_row
        except StopIteration:
            self._current_row = None

    def Close(self):
        self._reader.close()
        self._reader = None


def add_partition(connection, mprows, vid):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        mprows (mpf.MPRowsFile):

    """
    from apsw import MisuseError  # Moved into function to allow tests to run when it isn't installed

    module_name = 'mod_partition'
    try:
        connection.createmodule(module_name, _get_module_class(mprows)())
    except MisuseError:
        # TODO: The best solution I've found to check for existance. Try again later,
        # because MisuseError might mean something else.
        pass

    # create virtual table.
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE {} using {};'.format(table_name(vid), module_name))


def table_name(vid):
    """ Returns virtual table name for the given partition.

    Args:
        vid (str): vid of the partition

    Returns:
        str: name of the table associated with partition.

    """
    return 'p_{vid}_vt'.format(vid=vid)


def _get_module_class(mprows):
    """ Returns module class for the partition. """

    class Source:
        def Create(self, db, modulename, dbname, tablename, *args):
            columns_types = []
            column_names = []
            for column in sorted(mprows.reader.columns, key=lambda x: x['pos']):
                sqlite_type = TYPE_MAP.get(column['type'])
                if not sqlite_type:
                    raise Exception('Do not know how to convert {} to sql column.'.format(column['type']))
                columns_types.append('{} {}'.format(column['name'], sqlite_type))
                column_names.append(column['name'])
            columns_types_str = ',\n'.join(columns_types)
            schema = 'CREATE TABLE {}({})'.format(tablename, columns_types_str)
            return schema, Table(column_names, mprows)
        Connect = Create
    return Source
