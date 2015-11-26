# -*- coding: utf-8 -*-
import getpass
import logging
import operator
import re

from fs.opener import fsopendir

from six import binary_type, text_type

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG

from ambry_sources.mpf import MPRowsFile

POSTGRES_PARTITION_SCHEMA_NAME = 'partitions'
FOREIGN_SERVER_NAME = 'partition_server'

logger = logging.getLogger(__name__)

# python type to sqlite type map.
TYPE_MAP = {
    'int': 'INTEGER',
    'float': 'NUMERIC',
    binary_type.__name__: 'TEXT',
    text_type.__name__: 'TEXT',
    'date': 'DATE',
    'datetime': 'TIMESTAMP WITHOUT TIME ZONE'
}


def add_partition(cursor, mprows, vid):
    """ Creates foreign table for given partition.

    Args:
        cursor (psycopg2.cursor):
        mprows (mpf.MPRowsFile):
        vid (str): vid of the partition.
    """
    if not _postgres_shares_group():
        details_link = 'http://example.com/FIXME:'
        raise AssertionError(
            'postgres user does not have permission to read mpr file.\n'
            'Hint: postgres user should share group with user who executes ambry. See {} for details.'
            .format(details_link))
    _create_if_not_exists(cursor, FOREIGN_SERVER_NAME)
    query = _get_create_query(mprows, vid)
    logger.debug('Create foreign table for {} mprows. Query:\n{}.'.format(mprows.path, query))
    cursor.execute(query)


def _get_create_query(mprows, vid):
    """ Returns query to create foreign table.

    Args:
        mprows (mpf.MPRowsFile):
        vid (str): vid of the partition.

    Returns:
        str: sql query to craete foreign table.
    """
    columns = []
    for column in sorted(mprows.reader.columns, key=lambda x: x['pos']):
        postgres_type = TYPE_MAP.get(column['type'])
        if not postgres_type:
            raise Exception('Do not know how to convert {} to postgresql type.'.format(column['type']))
        columns.append('{} {}'.format(column['name'], postgres_type))

    query = """
        CREATE FOREIGN TABLE {table} (
            {columns}
        ) server {server_name} options (
            filesystem '{filesystem}',
            path '{path}'
        );
    """.format(table=table_name(vid),
               columns=',\n'.join(columns), server_name=FOREIGN_SERVER_NAME,
               filesystem=mprows._fs.root_path,
               path=mprows.path)
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
        logger.info('Create {} foreign server because it does not exist.'.format(server_name))
        query = """
            CREATE SERVER {} FOREIGN DATA WRAPPER multicorn
            options (
                wrapper 'ambry_sources.med.postgresql.MPRForeignDataWrapper'
            );
        """.format(server_name)
        cursor.execute(query)
    else:
        logger.debug('{} foreign server already exists. Do nothing.'.format(server_name))


def table_name(vid):
    """ Returns foreign table name for the given partition.

    Args:
        vid (str): vid of the partition

    Returns:
        str: name of the table associated with partition.

    """
    return '{schema}.p_{vid}_ft'.format(schema=POSTGRES_PARTITION_SCHEMA_NAME, vid=vid)


def _like_op(a, b):
    """ Returns True if 'a LIKE b'. """
    r_exp = b.replace('%', '.*').replace('_', '.{1}') + '$'
    return bool(re.match(r_exp, a))


def _ilike_op(a, b):
    """ Returns True if 'a ILIKE 'b. """
    return _like_op(a.lower(), b.lower())


def _not_like_op(a, b):
    """ Returns True if 'a NOT LIKE b'. """
    return not _like_op(a, b)


def _not_ilike_op(a, b):
    """ Returns True if 'a NOT LIKE b'. """
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

        if logger.level == logging.DEBUG:
            current_user = getpass.getuser()
            log_to_postgres(
                'Initializing Foreign Data Wrapper: user: {}, filesystem: {}, path: {}'
                .format(current_user, options['filesystem'], options['path']),
                DEBUG)
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
                    hint='Implement {} operator in the MPR FDW wrapper.'.format(qual.operator))
                continue

            elem_index = self.columns.index(qual.field_name)
            if not op(row[elem_index], qual.value):
                return False
        return True

    def execute(self, quals, columns):
        if logger.level == logging.DEBUG:
            syspath = self._mp_rows.syspath
            log_to_postgres(
                'Executing query over rows of the MPR: mpr: {}, quals: {}, columns: {}'
                .format(syspath, quals, columns),
                DEBUG)
            with self._mp_rows.reader as reader:
                for row in reader.rows:
                    assert isinstance(row, (tuple, list)), row

                    if not self._matches(quals, row):
                        log_to_postgres(
                            'No match, continue with another: mpr: {}, row: {}, quals: {}'
                            .format(syspath, row, quals),
                            DEBUG)
                        continue

                    log_to_postgres(
                        'Match found, yielding row {}: mpr: {}, quals: {}'
                        .format(syspath, row, quals),
                        DEBUG)

                    yield row
        else:
            # it is the same, except debug logging.
            with self._mp_rows.reader as reader:
                for row in reader.rows:
                    assert isinstance(row, (tuple, list)), row

                    if not self._matches(quals, row):
                        continue

                    yield row


def _postgres_shares_group():
    """ Returns True if postgres user shares group with app executor. Otherwise returns False.

    Returns:
        bool:

    """

    user = 'postgres'
    import getpass
    import grp
    import pwd
    current_user_group_id = pwd.getpwnam(getpass.getuser()).pw_gid
    current_user_group = grp.getgrgid(current_user_group_id).gr_name

    other_user_groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    return current_user_group in other_user_groups
