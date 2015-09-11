# -*- coding: utf-8 -*-
import logging

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
