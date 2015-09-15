# -*- coding: utf-8 -*-
import logging
from gzip import GzipFile
from datetime import datetime, time
import operator
import re

import msgpack

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING

POSTGRES_PARTITION_SCHEMA_NAME = 'partitions'

logger = logging.getLogger(__name__)


def add_partition(cursor, partition):
    """ Create foreign table for given partition.
    Args:
        connection (sqlalchemy.engine.Connection)
        partition (orm.Partition):
    """
    FOREIGN_SERVER_NAME = 'partition_server'
    _create_if_not_exists(cursor, FOREIGN_SERVER_NAME)
    columns = []
    # FIXME: Need to know format of the columns in the partition file.
    for column in partition.table.columns:
        if column.type == 'int':
            columns.append('{} integer'.format(column.name))
        elif column.type == 'str':
            columns.append('{} varchar'.format(column.name))
        elif column.type == 'date':
            columns.append('{} DATE'.format(column.name))
        elif column.type == 'datetime':
            columns.append('{} TIMESTAMP WITHOUT TIME ZONE'.format(column.name))
        else:
            raise Exception('Do not know how to convert {} to sql column.'.format(column.type))

    query = """
        CREATE FOREIGN TABLE {table_name} (
            {columns}
        ) server {server_name} options (
            filename '{file_name}'
        );
    """.format(table_name=_table_name(partition),
               columns=',\n'.join(columns), server_name=FOREIGN_SERVER_NAME,
               file_name=partition.datafile.syspath)
    logging.debug('Create foreign table for {} partition. Query:\n{}.'.format(partition.vid, query))
    cursor.execute(query)


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
                wrapper 'ambryfdw.PartitionMsgpackForeignDataWrapper'
            );
        """.format(server_name)
        cursor.execute(query)
    else:
        logging.debug('{} foreign server already exists. Do nothing.'.format(server_name))


def _table_name(partition):
    """ Returns foreign table name for the given partition. """
    return '{schema}.p_{vid}_ft'.format(schema=POSTGRES_PARTITION_SCHEMA_NAME, vid=partition.vid)


# Note:
#    date and time formats listed here have to match to formats used in the
#    ambry.etl.partition.PartitionMsgpackDataFileReader.decode_obj

DATETIME_FORMAT_NO_MS = '%Y-%m-%dT%H:%M:%S'
DATETIME_FORMAT_WITH_MS = '%Y-%m-%dT%H:%M:%S.%f'
TIME_FORMAT = '%H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'


def like_op(a, b):
    """ Returns True if 'a LIKE b'. """
    # FIXME: Optimize
    r_exp = b.replace('%', '.*').replace('_', '.{1}') + '$'
    return bool(re.match(r_exp, a))


def ilike_op(a, b):
    """ Returns True if 'a ILIKE 'b. FIXME: is it really ILIKE? """
    return like_op(a.lower(), b.lower())


def not_like_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not like_op(a, b)


def not_ilike_op(a, b):
    """ Returns True if 'a NOT LIKE b'. FIXME: is it really NOT? """
    return not ilike_op(a, b)


QUAL_OPERATOR_MAP = {
    '=': operator.eq,
    '<': operator.lt,
    '>': operator.gt,
    '<=': operator.le,
    '>=': operator.ge,
    '<>': operator.ne,
    '~~': like_op,
    '~~*': ilike_op,
    '!~~*': not_ilike_op,
    '!~~': not_like_op,
}


class PartitionMsgpackForeignDataWrapper(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(PartitionMsgpackForeignDataWrapper, self).__init__(options, columns)
        self.columns = columns
        if 'filename' not in options:
            log_to_postgres(
                'Filename is required option of the partition msgpack fdw.',
                ERROR,
                hint='Try adding the missing option in the table creation statement')  # FIXME:
            raise RuntimeError('filename is required option of the partition msgpack fdw.')
        self.filename = options['filename']

    @staticmethod
    def decode_obj(obj):
        if b'__datetime__' in obj:
            # FIXME: not tested
            try:
                obj = datetime.strptime(obj['as_str'], DATETIME_FORMAT_NO_MS)
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.strptime(obj['as_str'], DATETIME_FORMAT_WITH_MS)
        elif b'__time__' in obj:
            # FIXME: not tested
            obj = time(*list(time.strptime(obj['as_str'], TIME_FORMAT))[3:6])
        elif b'__date__' in obj:
            # FIXME: not tested
            obj = datetime.strptime(obj['as_str'], DATE_FORMAT).date()
        else:
            # FIXME: not tested
            raise Exception('Unknown type on decode: {} '.format(obj))
        return obj

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
        with open(self.filename, 'rb') as stream:
            unpacker = msgpack.Unpacker(GzipFile(fileobj=stream), object_hook=self.decode_obj)
            header = None

            for row in unpacker:
                assert isinstance(row, (tuple, list)), row

                if not header:
                    header = row
                    continue

                if not self._matches(quals, row):
                    continue

                yield row
