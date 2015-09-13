# -*- coding: utf-8 -*-
from datetime import datetime, date

import msgpack
import gzip

from apsw import MisuseError

from ambry_sources.mpf import MPRowsFile

# Documents used to implement module and function:
# Module: http://apidoc.apsw.googlecode.com/hg/vtable.html
# Functions: http://www.drdobbs.com/database/query-anything-with-sqlite/202802959?pgno=3


def get_module_class(partition):
    """ Returns module class for the partition. """

    class Source:
        def Create(self, db, modulename, dbname, tablename, *args):
            columns_types = []
            column_names = []
            for column in sorted(partition.schema, key=lambda x: x['pos']):
                # FIXME: Use map.
                if column['type'] == 'int':
                    columns_types.append('{} integer'.format(column['name']))
                elif column['type'] == 'str':
                    columns_types.append('{} varchar'.format(column['name']))
                elif column['type'] == 'date':
                    columns_types.append('{} DATE'.format(column['name']))
                elif column['type'] == 'datetime':
                    columns_types.append('{} TIMESTAMP WITHOUT TIME ZONE'.format(column['name']))
                else:
                    raise Exception('Do not know how to convert {} to sql column.'.format(column['type']))
                column_names.append(column['name'])
            columns_types_str = ',\n'.join(columns_types)
            schema = 'CREATE TABLE {}({})'.format(tablename, columns_types_str)
            return schema, Table(column_names, partition)
        Connect = Create
    return Source


class Table:
    """ Represents a table """
    def __init__(self, columns, partition):
        """

        Args:
            columns (list of str): column names
            partition (mpf.MprRowsFile):

        """
        self.columns = columns
        self.partition = partition

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
        self._reader = table.partition.reader
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
            assert isinstance(self._current_row, (tuple, list)), self._current_row
        except StopIteration:
            self._current_row = None

    def Close(self):
        self._reader.close()
        self._reader = None


def add_partition(connection, partition):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        partition (mpf.MprRowsFile):

    """

    module_name = 'mod_partition'
    try:
        connection.createmodule(module_name, get_module_class(partition)())
    except MisuseError:
        # TODO: The best solution I've found to check for existance. Try again later,
        # because MisuseError might mean something else.
        pass

    # create virtual table.
    cursor = connection.cursor()
    cursor.execute('CREATE VIRTUAL TABLE {} using {};'.format(_table_name(partition), module_name))


def _table_name(partition):
    """ Returns virtual table name for the given partition. """
    # FIXME: find the better naming.
    return 'p_{name}_{p_vid}_vt'.format(name='_'.join(partition.headers), p_vid=partition.partition_vid or '')
