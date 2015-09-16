# -*- coding: utf-8 -*-
import unittest

import psycopg2

from fs.opener import fsopendir

from ambry_sources import get_source
from ambry_sources.med.postgresql import add_partition, _table_name
from ambry_sources.mpf import MPRowsFile

from tests import PostgreSQLTestBase, TestBase


class Test(TestBase):

    def test_creates_foreign_data_table_for_simple_fixed_mpr(self):
        # build rows reader
        cache_fs = fsopendir(self.setup_temp_dir())
        sources = self.load_sources()
        spec = sources['simple_fixed']
        s = get_source(spec, cache_fs)
        partition = MPRowsFile(cache_fs, spec.name).load_rows(s)

        try:
            # create foreign data table
            PostgreSQLTestBase._create_postgres_test_db()
            conn = psycopg2.connect(**PostgreSQLTestBase.pg_test_db_data)

            try:
                with conn.cursor() as cursor:
                    # we have to close opened transaction.
                    cursor.execute('commit;')
                    add_partition(cursor, partition)

                # try to query just added partition foreign data table.
                with conn.cursor() as cursor:
                    table_name = _table_name(partition)
                    cursor.execute('SELECT rowid, col1, col2 from {};'.format(table_name))
                    result = cursor.fetchall()
                    self.assertEqual(len(result), 100)
                    self.assertEqual(result[0], (0, 0, '0'))
                    self.assertEqual(result[-1], (99, 99, '99'))
            finally:
                conn.close()
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()
