# -*- coding: utf-8 -*-

import psycopg2

from fs.opener import fsopendir

from ambry_sources import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec
from ambry_sources.med.postgresql import add_partition, table_name

from tests import PostgreSQLTestBase, TestBase

# https://github.com/CivicKnowledge/ambry_sources/issues/20


class Test(TestBase):

    def test_executes_select_query_without_any_error(self):

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
                cursor.execute('SELECT * FROM {};'.format(table_name('vid1')))
        finally:
            if connection:
                connection.close()
            PostgreSQLTestBase._drop_postgres_test_db()
