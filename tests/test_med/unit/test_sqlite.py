# -*- coding: utf-8 -*-

from datetime import datetime
import unittest

from attrdict import AttrDict

import six

import fudge
from fudge.inspector import arg

from ambry_sources.med.sqlite import add_partition, _get_module_class, Table, Cursor


class TestTable(unittest.TestCase):
    def test_returns_cursor(self):
        columns = []
        partition = AttrDict({
            'reader': {
                'rows': [[1]]}})
        table = Table(columns, partition)
        cursor = table.Open()
        self.assertTrue(hasattr(cursor, 'Next'))
        self.assertTrue(hasattr(cursor, 'Column'))


class TestCursor(unittest.TestCase):

    def _get_fake_table(self, rows=None, reader=None):
        if not rows:
            rows = [[1.1, 1.2], [2.1, 2.2], [3.1, 3.2]]
        if not reader:
            reader = {'rows': rows, 'close': lambda x: None}

        table = AttrDict({
            'mprows': {
                'reader': reader}})
        return table

    # Eof test
    def test_returns_False_if_current_row_is_not_empty(self):
        table = self._get_fake_table()
        self.assertTrue(Cursor(table).Eof)

    def test_returns_True_if_there_is_now_no_current_row(self):
        table = self._get_fake_table()
        cursor = Cursor(table)
        cursor._current_row = None

    # Rowid test
    def test_Rowid_returns_row_number(self):
        table = self._get_fake_table()
        cursor = Cursor(table)
        self.assertEqual(cursor.Rowid(), 1)

    # Column test
    def test_returns_value_by_column_index(self):
        table = self._get_fake_table()
        cursor = Cursor(table)
        self.assertEqual(cursor.Column(0), 1.1)
        self.assertEqual(cursor.Column(1), 1.2)

    def test_converts_datetime_value_to_isoformat(self):
        dt = datetime(2010, 10, 10, 10, 10, 10)
        table = self._get_fake_table(rows=[[dt]])
        cursor = Cursor(table)
        self.assertEqual(cursor.Column(0), '2010-10-10T10:10:10')

    # Next tests
    def test_calling_next_moves_cursor_to_next_row(self):
        table = self._get_fake_table()
        cursor = Cursor(table)
        self.assertEqual(cursor.Column(0), 1.1)
        cursor.Next()
        self.assertEqual(cursor.Column(0), 2.1)

    def test_calling_next_empties_current_row_if_there_is_no_next_row(self):
        table = self._get_fake_table(rows=[[1.1]])
        cursor = Cursor(table)
        self.assertEqual(cursor.Column(0), 1.1)
        self.assertIsNotNone(cursor._current_row)
        cursor.Next()
        self.assertIsNone(cursor._current_row)

    # Close tests
    def test_closes_and_empties_reader(self):
        table = self._get_fake_table()
        cursor = Cursor(table)
        with fudge.patched_context(cursor._reader, 'close', fudge.Fake('close').expects_call()):
            cursor.Close()
            fudge.verify()
        self.assertIsNone(cursor._reader)


class AddPartitionTest(unittest.TestCase):

    @fudge.patch(
        'ambry_sources.med.sqlite._get_module_class',
        'ambry_sources.med.sqlite.table_name')
    def test_creates_sqlite_module(self, fake_get, fake_table):
        fake_get.expects_call().returns(fudge.Fake().is_a_stub())
        fake_table.expects_call()
        fake_create_module = fudge.Fake('createmodule').expects_call()
        fake_connection = AttrDict({
            'createmodule': fake_create_module,
            'cursor': lambda: fudge.Fake().is_a_stub()})
        fake_mprows = AttrDict()
        add_partition(fake_connection, fake_mprows, 'vid1')

    @fudge.patch(
        'ambry_sources.med.sqlite._get_module_class',
        'ambry_sources.med.sqlite.table_name')
    def test_creates_virtual_table(self, fake_get, fake_table):
        fake_get.expects_call().returns(fudge.Fake().is_a_stub())
        fake_table.expects_call()
        fake_create_module = fudge.Fake('createmodule').expects_call()
        fake_execute = fudge.Fake().expects_call().with_args(arg.contains('CREATE VIRTUAL TABLE'))
        fake_connection = AttrDict({
            'createmodule': fake_create_module,
            'cursor': lambda: AttrDict({'execute': fake_execute})})
        fake_mprows = AttrDict()
        add_partition(fake_connection, fake_mprows, 'vid1')


class GetModuleClassTest(unittest.TestCase):

    def test_returns_source_class(self):
        cls = _get_module_class(AttrDict({}))
        self.assertTrue(hasattr(cls, 'Create'))
        self.assertTrue(six.callable(cls.Create))

    def _get_fake_partition(self, type_):
        partition = AttrDict({
            'reader': AttrDict({
                'columns': [{'type': type_, 'name': 'column1', 'pos': 0}]})})
        return partition

    # Source.Create tests
    def test_returns_create_table_query_and_table(self):
        mprows = self._get_fake_partition('int')
        cls = _get_module_class(mprows)
        ret = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertEqual(len(ret), 2)
        query, table = ret
        self.assertEqual('CREATE TABLE table1(column1 INTEGER)', query)
        self.assertTrue(hasattr(table, 'Open'))

    def test_converts_int_to_integer_sqlite_type(self):
        mprows = self._get_fake_partition('int')
        cls = _get_module_class(mprows)
        query, table = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertIn('(column1 INTEGER)', query)

    def test_converts_float_to_real_sqlite_type(self):
        mprows = self._get_fake_partition('float')
        cls = _get_module_class(mprows)
        query, table = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertIn('(column1 REAL)', query)

    def test_converts_str_to_text_sqlite_type(self):
        mprows = self._get_fake_partition('str')
        cls = _get_module_class(mprows)
        query, table = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertIn('(column1 TEXT)', query)

    def test_converts_date_to_date_sqlite_type(self):
        mprows = self._get_fake_partition('date')
        cls = _get_module_class(mprows)
        query, table = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertIn('(column1 DATE)', query)

    def test_converts_datetime_to_timestamp_sqlite_type(self):
        mprows = self._get_fake_partition('datetime')
        cls = _get_module_class(mprows)
        query, table = cls().Create('db', 'modulename', 'dbname', 'table1')
        self.assertIn('(column1 TIMESTAMP WITHOUT TIME ZONE)', query)

    def test_raises_exception_if_type_conversion_failed(self):
        mprows = self._get_fake_partition('unknown')
        cls = _get_module_class(mprows)
        try:
            cls().Create('db', 'modulename', 'dbname', 'table1')
        except Exception as exc:
            self.assertIn('Do not know how to convert', str(exc))
