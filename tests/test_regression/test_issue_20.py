# -*- coding: utf-8 -*-

try:
    # py2, mock is external lib.
    from mock import patch
except ImportError:
    # py3, mock is included
    from unittest.mock import patch

import psycopg2

from fs.opener import fsopendir

from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec
from ambry_sources.med.postgresql import add_partition

from tests import PostgreSQLTestBase, TestBase

# https://github.com/CivicKnowledge/ambry_sources/issues/20


class Test(TestBase):

    @patch('ambry_sources.med.postgresql._postgres_shares_group')
    def test_executes_select_query_without_any_error(self, fake_shares):
        fake_shares.return_value = True

        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate first row
            yield [0, 0]

        fs = fsopendir('temp://')
        datafile = MPRowsFile(fs, 'vid1')
        datafile.load_rows(GeneratorSource(SourceSpec('foobar'), gen()))
        connection = None
        try:
            PostgreSQLTestBase._create_postgres_test_db()
            connection = psycopg2.connect(**PostgreSQLTestBase.pg_test_db_data)

            # create foreign table for partition
            with connection.cursor() as cursor:
                # we have to close opened transaction.
                cursor.execute('COMMIT;')
                add_partition(cursor, datafile, 'vid1')

            # query just created foreign table.
            with connection.cursor() as cursor:
                cursor.execute('SELECT * FROM partitions.vid1;')
        finally:
            if connection:
                connection.close()
            PostgreSQLTestBase._drop_postgres_test_db()
