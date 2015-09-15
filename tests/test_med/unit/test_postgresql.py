# -*- coding: utf-8 -*-
from datetime import date, datetime

import psycopg2

from ambry_sources.med.postgresql import add_partition, _table_name

from tests import PostgreSQLTestBase
from tests import TestBase


class _Test(object): # is disabled because it is broken
# class Test(TestBase):  # FIXME: uncomment and fix.

    def test_creates_table(self):
        # create fake partition.
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)

        # testing.
        try:
            PostgreSQLTestBase._create_postgres_test_db()
            conn = psycopg2.connect(**PostgreSQLTestBase.pg_test_db_data)

            try:
                with conn.cursor() as cursor:
                    # we have to close opened transaction.
                    cursor.execute('commit;')
                    add_partition(cursor, partition)

                # try to query just added partition virtual table.
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

    def test_creates_many_tables(self):
        try:
            PostgreSQLTestBase._create_postgres_test_db()
            conn = psycopg2.connect(**PostgreSQLTestBase.pg_test_db_data)

            try:
                partitions = []
                with conn.cursor() as cursor:
                    for i in range(100):
                        partition_vid = 'vid_{}'.format(i)
                        partition = self._get_fake_partition(partition_vid)
                        add_partition(cursor, partition)
                        partitions.append(partition)

                # check all virtual tables and rows.
                with conn.cursor() as cursor:
                    for partition in partitions:
                        table_name = _table_name(partition)
                        query = 'SELECT * FROM {};'.format(table_name)
                        cursor.execute(query)
                        result = cursor.fetchall()
                        self.assertEqual(len(result), 100)
            finally:
                conn.close()
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()

    def test_date_and_datetime(self):
        # create fake partition.
        partition_vid = 'vid1'
        partition = self._get_fake_datetime_partition(partition_vid)

        # testing.
        try:
            PostgreSQLTestBase._create_postgres_test_db()
            conn = psycopg2.connect(**PostgreSQLTestBase.pg_test_db_data)

            try:
                with conn.cursor() as cursor:
                    add_partition(cursor, partition)

                with conn.cursor() as cursor:
                    # select from FDW table.
                    table_name = _table_name(partition)
                    cursor.execute('SELECT rowid, col1, col2 from {};'.format(table_name))
                    result = cursor.fetchall()
                    self.assertEqual(len(result), 100)
                    self.assertEqual(
                        result[0],
                        (0, date(2015, 8, 30), datetime(2015, 8, 30, 11, 41, 32, 977993)))
            finally:
                conn.close()
        finally:
            PostgreSQLTestBase._drop_postgres_test_db()
