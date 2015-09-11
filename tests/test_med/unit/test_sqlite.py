# -*- coding: utf-8 -*-

import apsw

from six import u, b

from ambry_sources.med.sqlite import add_partition, _table_name

from tests.test_med import BaseMEDTest


class Test(BaseMEDTest):

    def test_creates_virtual_table(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[-1], (99, b('99')))

    def test_many_queries_on_one_partition(self):
        partition_vid = 'vid1'
        partition = self._get_fake_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select all from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[-1], (99, b('99')))

        # select first three records
        query = 'SELECT col1, col2 FROM {} LIMIT 3;'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], (0, b('0')))
        self.assertEqual(result[1], (1, b('1')))
        self.assertEqual(result[2], (2, b('2')))

        # select with filtering
        query = 'SELECT col1 FROM {} WHERE col1=\'1\';'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (1,))

    def test_creates_virtual_table_for_each_partition(self):
        partitions = []
        connection = apsw.Connection(':memory:')
        for i in range(1000):
            partition_vid = 'vid_{}'.format(i)
            partition = self._get_fake_partition(partition_vid)
            add_partition(connection, partition)
            partitions.append(partition)

        # check all tables and rows.
        cursor = connection.cursor()
        for partition in partitions:
            query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
            result = cursor.execute(query).fetchall()
            self.assertEqual(len(result), 100)
            self.assertEqual(result[0], (0, b('0')))
            self.assertEqual(result[-1], (99, b('99')))

    def test_date_and_datetime(self):
        partition_vid = 'vid1'
        partition = self._get_fake_datetime_partition(partition_vid)
        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (u('2015-08-30'), u('2015-08-30T11:41:32.977993')))
