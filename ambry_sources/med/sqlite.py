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
            for i, column in enumerate(partition.table.columns):
                if i == 0:
                    # First column is already reserved for rowid. This is current release constraint
                    # and will be removed when I discover real partitions data more deeply.
                    continue

                # FIXME: Need to know format of the columns in the partition file.
                if column.type == 'int':
                    columns_types.append('{} integer'.format(column.name))
                elif column.type == 'str':
                    columns_types.append('{} varchar'.format(column.name))
                elif column.type == 'date':
                    columns_types.append('{} DATE'.format(column.name))
                elif column.type == 'datetime':
                    columns_types.append('{} TIMESTAMP WITHOUT TIME ZONE'.format(column.name))
                else:
                    raise Exception('Do not know how to convert {} to sql column.'.format(column.type))

                column_names.append(column.name)
            columns_types_str = ',\n'.join(columns_types)
            schema = 'CREATE TABLE {}({})'.format(tablename, columns_types_str)
            return schema, Table(column_names, partition.datafile.syspath)
        Connect = Create
    return Source


class Table:
    """ Represents a table """
    def __init__(self, columns, filename):
        self.columns = columns
        self.filename = filename

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
        self._current_row = None
        self._next_row = None
        self._f = open(table.filename, 'rb')
        self._msg_file = gzip.GzipFile(fileobj=self._f)
        self._unpacker = msgpack.Unpacker(
            self._msg_file, object_hook=MPRowsFile.decode_obj)
        self._header = next(self._unpacker)
        self._current_row = next(self._unpacker)

    def Filter(self, *args):
        pass

    def Eof(self):
        return self._current_row is None

    def Rowid(self):
        return self._current_row[0]

    def Column(self, col):
        value = self._current_row[1 + col]
        if isinstance(value, (date, datetime)):
            # Convert to ISO format.
            return value.isoformat()
        return value

    def Next(self):
        try:
            self._current_row = next(self._unpacker)
            assert isinstance(self._current_row, (tuple, list)), self._current_row
        except StopIteration:
            self._current_row = None

    def Close(self):
        self._f.close()
        self._unpacker = None


def add_partition(connection, partition):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        partition (ambry.orm.Partiton):

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
    return 'p_{vid}_vt'.format(vid=partition.vid)
