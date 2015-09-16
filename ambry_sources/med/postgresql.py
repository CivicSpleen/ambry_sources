# -*- coding: utf-8 -*-
import logging
import operator
import re
from fs.opener import fsopendir
from ambry_sources.mpf import MPRowsFile

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING

POSTGRES_PARTITION_SCHEMA_NAME = 'partitions'
FOREIGN_SERVER_NAME = 'partition_server'

logger = logging.getLogger(__name__)

# python type to sqlite type map.
TYPE_MAP = {
    'int': 'INTEGER',
    'float': 'NUMERIC',
    'str': 'TEXT',
    'date': 'DATE',
    'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
}


def add_partition(cursor, partition):
    """ Creates foreign table for given partition.

    Args:
        cursor (FIXME:):
        partition (FIXME:):
    """
    _create_if_not_exists(cursor, FOREIGN_SERVER_NAME)
    query = _get_create_query(partition)
    logging.debug('Create foreign table for {} partition. Query:\n{}.'.format(partition.path, query))
    cursor.execute(query)


def _get_create_query(partition):
    """ Returns query to create foreign table.

    Args:
        connection (sqlalchemy.engine.Connection)

    Returns:
        str: sql query to craete foreign table.
    """
    columns = []
    for column in sorted(partition.schema, key=lambda x: x['pos']):
        postgres_type = TYPE_MAP.get(column['type'])
        if not postgres_type:
            raise Exception('Do not know how to convert {} to postgresql type.'.format(column['type']))
        columns.append('{} {}'.format(column['name'], postgres_type))

    query = """
        CREATE FOREIGN TABLE {table_name} (
            {columns}
        ) server {server_name} options (
            filesystem '{filesystem}'
            path '{path}
        );
    """.format(table_name=_table_name(partition),
               columns=',\n'.join(columns), server_name=FOREIGN_SERVER_NAME,
               filesystem='fs',  # FIXME: give valid filesystem.
               path='path')  # FIXME: give valid path.
    return query


def _server_exists(cursor, server_name):
    """ Returns True is foreign server with given name exists. Otherwise returns False. """
    query = """
        SELECT 1 FROM pg_foreign_server WHERE srvname=%s;
    """
    cursor.execute(query, [server_name])
    return cursor.fetchall() == [(1,)]


def _create_if_not_exists(cursor, server_name):
    """ Creates foreign server if it does not exist. """
    if not _server_exists(cursor, server_name):
        logging.info('Create {} foreign server because it does not exist.'.format(server_name))
        query = """
            CREATE SERVER {} FOREIGN DATA WRAPPER multicorn
            options (
                wrapper 'ambry_sources.med.postgresql.MPRForeignDataWrapper'
            );
        """.format(server_name)
        cursor.execute(query)
    else:
        logging.debug('{} foreign server already exists. Do nothing.'.format(server_name))


def _table_name(partition):
    """ Returns foreign table name for the given partition.

    Args:
        partition (mpf.MprRowsFile):

    Returns:
        str: name of the table associated with partition.

    """
    # FIXME: find the better naming.
    name = partition.path.replace('.', '_').replace(' ', '_')
    return '{schema}.{name}_ft'.format(schema=POSTGRES_PARTITION_SCHEMA_NAME, name=name)


def _like_op(a, b):
    """ Returns True if 'a LIKE b'. """
    # FIXME: Optimize
    r_exp = b.replace('%', '.*').replace('_', '.{1}') + '$'
    return bool(re.match(r_exp, a))


def _ilike_op(a, b):
    """ Returns True if 'a ILIKE 'b. FIXME: is it really ILIKE? """
    return _like_op(a.lower(), b.lower())


def _not_like_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not _like_op(a, b)


def _not_ilike_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not _ilike_op(a, b)


QUAL_OPERATOR_MAP = {
    '=': operator.eq,
    '<': operator.lt,
    '>': operator.gt,
    '<=': operator.le,
    '>=': operator.ge,
    '<>': operator.ne,
    '~~': _like_op,
    '~~*': _ilike_op,
    '!~~*': _not_ilike_op,
    '!~~': _not_like_op,
}


class MPRForeignDataWrapper(ForeignDataWrapper):
    """ Message Pack Rows (MPR) foreign data wrapper. """

    def __init__(self, options, columns):
        """

        Args:
            options (dict): filesystem and path, filesystem is root directory str, path is relative
                name of the file.
                Example: {
                    'filesystem': '/tmp/my-root',
                    'path': '/dir1/file1.mpr'
                }
        """

        super(MPRForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        if 'path' not in options:
            log_to_postgres(
                'Filename is required option of the partition msgpack fdw.',
                ERROR,
                hint='Try to add the `path` option to the table creation statement')
            raise RuntimeError('`path` is required option of the MPR (Message Pack Rows) fdw.')

        if 'filesystem' not in options:
            log_to_postgres(
                'filesystem is required option of the partition msgpack fdw.',
                ERROR,
                hint='Try to add the `filesystem` option to the table creation statement')
            raise RuntimeError('`filesystem` is required option of the MPR (Message Pack Rows) fdw.')
        self.filesystem = fsopendir(options['filesystem'])
        self.path = options['path']
        self._mp_rows = MPRowsFile(self.filesystem, self.path)

    def _matches(self, quals, row):
        """ Returns True if row matches to all quals. Otherwise returns False.

        Args:
            quals (list of Qual):
            row (list or tuple):

        Returns:
            bool: True if row matches to all quals, False otherwise.
        """
        for qual in quals:
            op = QUAL_OPERATOR_MAP.get(qual.operator)
            if op is None:
                log_to_postgres(
                    'Unknown operator {} in the {} qual. Row will be returned.'.format(qual.operator, qual),
                    WARNING,
                    hint='Implement that operator in the ambryfdw wrapper.')
                continue

            elem_index = self.columns.index(qual.field_name)
            if not op(row[elem_index], qual.value):
                return False
        return True

    def execute(self, quals, columns):
        with self._mp_rows.reader as reader:
            for row in reader.rows:
                assert isinstance(row, (tuple, list)), row

                if not self._matches(quals, row):
                    continue

                yield row
