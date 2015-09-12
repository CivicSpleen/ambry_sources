# -*- coding: utf-8 -*-

import apsw

from six import u

from ambry_sources.med.sqlite import add_partition, _table_name

from tests.test_med import BaseMEDTest


class Test(BaseMEDTest):

    def test_creates_virtual_table(self):
        # first validate file matches my expactations.
        f = self._get_partition_mpr('col1_int_col2_str_100_rows.csv')
        types = [x['type'] for x in f.schema]
        names = [x['name'] for x in f.schema]
        assert 'int' in types
        assert 'str' in types
        assert 'col1' in names
        assert 'col2' in names

        # create virtual table
        connection = apsw.Connection(':memory:')
        add_partition(connection, f)

        # select from just created virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(f))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)

        # FIXME: First column has to be integer.
        self.assertEqual(result[0], (u('1'), u('row1')))
        self.assertEqual(result[-1], (u('100'), u('row100')))

    def test_many_queries_on_one_partition(self):
        # first validate file matches my expactations.
        f = self._get_partition_mpr('col1_int_col2_str_100_rows.csv')
        types = [x['type'] for x in f.schema]
        names = [x['name'] for x in f.schema]
        assert 'int' in types
        assert 'str' in types
        assert 'col1' in names
        assert 'col2' in names

        # create virtual table
        connection = apsw.Connection(':memory:')
        add_partition(connection, f)

        # select all from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col1, col2 FROM {};'.format(_table_name(f))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (u('1'), u('row1')))
        self.assertEqual(result[-1], (u('100'), u('row100')))

        # select first three records
        query = 'SELECT col1, col2 FROM {} LIMIT 3;'.format(_table_name(f))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 3)
        # FIXME: First column elems have to be integers.
        self.assertEqual(result[0], (u('1'), u('row1')))
        self.assertEqual(result[1], (u('2'), u('row2')))
        self.assertEqual(result[2], (u('3'), u('row3')))

        # select with filtering
        query = 'SELECT col1 FROM {} WHERE col1=\'1\';'.format(_table_name(f))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 1)
        # FIXME: col1 elems have to be integers.
        self.assertEqual(result[0], (u('1'),))

    def test_creates_virtual_table_for_each_partition(self):
        partitions = []
        connection = apsw.Connection(':memory:')
        for i in range(20):
            partition_vid = 'vid_{}'.format(i)
            f = self._get_partition_mpr('col1_int_col2_str_100_rows.csv', partition_vid=partition_vid)
            add_partition(connection, f)
            partitions.append(f)

        # check all tables and rows.
        cursor = connection.cursor()
        for partition in partitions:
            query = 'SELECT col1, col2 FROM {};'.format(_table_name(partition))
            result = cursor.execute(query).fetchall()
            self.assertEqual(len(result), 100)
            self.assertEqual(result[0], (u('1'), u('row1')))
            self.assertEqual(result[-1], (u('100'), u('row100')))

    def test_date_and_datetime(self):
        f = self._get_partition_mpr('col1_date_col2_datetime_100_rows.csv')
        # f = self._get_partition_mpr('col1_int_col2_str_100_rows.csv')
        # FIXME: row intuiter can't recognize header of the file. Ask Eric is it bug or feature.

        # first validate file matches my expactations.
        types = [x['type'] for x in f.schema]
        names = [x['name'] for x in f.schema]
        assert 'date' in types
        assert 'datetime' in types
        assert 'col0' in names
        assert 'col1' in names

        # create virtual table.
        connection = apsw.Connection(':memory:')
        add_partition(connection, f)

        # select from virtual table.
        cursor = connection.cursor()
        query = 'SELECT col0, col1 FROM {};'.format(_table_name(f))
        result = cursor.execute(query).fetchall()
        # FIXME: result should not contain header
        # drop header
        result = result[1:]
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0], (u('2015-08-30'), u('2015-08-30T11:41:32')))
