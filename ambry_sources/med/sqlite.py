# -*- coding: utf-8 -*-
from datetime import datetime, date

from fs.opener import fsopendir

from six import binary_type, text_type

from ambry_sources.mpf import MPRowsFile

from ambry.util import get_logger
import logging
logger = get_logger(__name__, level=logging.INFO, propagate=False)

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

MODULE_NAME = 'mod_partition'



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


def install_mpr_module(connection):
    """ Install module which allow to execute queries over mpr files.

    Args:
        connection (apsw.Connection):

    """
    from apsw import MisuseError  # Moved into function to allow tests to run when it isn't installed

    try:
        connection.createmodule(MODULE_NAME, _get_module_instance())
    except MisuseError:
        # TODO: The best solution I've found to check for existance. Try again later,
        # because MisuseError might mean something else.
        pass


def add_partition(connection, mprows, vid):
    """ Creates virtual table for partition.

    Args:
        connection (apsw.Connection):
        mprows (mpf.MPRowsFile):

    """
    install_mpr_module(connection)

    # create a virtual table.
    cursor = connection.cursor()
    # drop extension because some partition may fail with SQLError: SQLError: unrecognized token:
    # See https://github.com/CivicKnowledge/ambry_sources/issues/22 for details.
    # MPRows implementation is clever enough to restore partition before reading.
    path = mprows.path
    if path.endswith('.mpr'):
        path = path[:-4]

    query = 'CREATE VIRTUAL TABLE {table} using {module}({filesystem}, {path});'\
            .format(table=table_name(vid), module=MODULE_NAME,
                    filesystem=mprows._fs.root_path, path=path)
    try:
        cursor.execute(query)
    except Exception as e:
        logger.warn("While adding a partition to sqlite warehouse, failed to exec '{}' ".format(query))
        raise


def table_name(vid):
    """ Returns virtual table name for the given partition.

    Args:
        vid (str): vid of the partition

    Returns:
        str: name of the table associated with the partition.

    """
    return 'p_{vid}_vt'.format(vid=vid)


def _get_module_instance():
    """ Returns module instance for the partitions virtual tables.

    Note:
        There is only one module for all virtual tables.

    """

    class Source:
        def Create(self, db, modulename, dbname, tablename, filesystem_root, path, *args):
            filesystem = fsopendir(filesystem_root)
            mprows = MPRowsFile(filesystem, path)
            columns_types = []
            column_names = []
            for column in sorted(mprows.reader.columns, key=lambda x: x['pos']):
                sqlite_type = TYPE_MAP.get(column['type'])
                if not sqlite_type:
                    raise Exception('Do not know how to convert {} to sql column.'.format(column['type']))
                columns_types.append('{} {}'.format(column['name'], sqlite_type))
                column_names.append(column['name'])
            columns_types_str = ',\n'.join(columns_types)
            schema = 'CREATE TABLE {}({});'.format(tablename, columns_types_str)

            return schema, Table(column_names, mprows)
        Connect = Create

    return Source()
