# -*- coding: utf-8 -*-
from attrdict import AttrDict

from multicorn.utils import WARNING

try:
    # py2, mock is external lib.
    from mock import patch, Mock
except ImportError:
    # py3, mock is included
    from unittest.mock import patch, Mock

from ambry_sources.mpf import MPRowsFile

from ambry_sources.med.postgresql import add_partition, _get_create_query, MPRForeignDataWrapper

from tests import TestBase


class AddPartitionTest(TestBase):

    @patch('ambry_sources.med.postgresql._create_if_not_exists')
    @patch('ambry_sources.med.postgresql._postgres_shares_group')
    def test_creates_foreign_server(self, fake_shares, fake_create):
        fake_shares.return_value = True
        cursor = AttrDict({
            'execute': lambda q: None})
        mprows = _get_fake_partition()
        add_partition(cursor, mprows, 'vid1')
        self.assertEqual(fake_create.call_count, 1)

    @patch('ambry_sources.med.postgresql._create_if_not_exists')
    @patch('ambry_sources.med.postgresql._postgres_shares_group')
    def test_creates_foreign_table(self, fake_shares, fake_create):
        fake_shares.return_value = True
        fake_execute = Mock()
        cursor = AttrDict({
            'execute': fake_execute})
        mprows = _get_fake_partition()
        add_partition(cursor, mprows, 'vid1')
        self.assertEqual(fake_execute.call_count, 1)
        self.assertIn('CREATE FOREIGN TABLE', str(fake_execute.mock_calls[0]))

    @patch('ambry_sources.med.postgresql._postgres_shares_group')
    def test_raises_exception_if_postgres_user_does_not_have_read_permission(self, fake_shares):
        fake_shares.return_value = False
        cursor = AttrDict({'execute': lambda q: None})
        mprows = _get_fake_partition()
        raises = False
        try:
            add_partition(cursor, mprows, 'vid1')
        except AssertionError as exc:
            raises = True
            self.assertIn('postgres user will not have permission to read mpr file.', str(exc))
        self.assertTrue(raises)


class GetCreateQueryTest(TestBase):
    def test_converts_int_to_postgresql_type(self):
        mprows = _get_fake_partition(type_='int')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('column1 INTEGER', query)

    def test_converts_float_to_postgresql_type(self):
        mprows = _get_fake_partition(type_='float')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('column1 NUMERIC', query)

    def test_converts_str_to_postgresql_type(self):
        mprows = _get_fake_partition(type_='str')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('column1 TEXT', query)

    def test_converts_date_to_postgresql_type(self):
        mprows = _get_fake_partition(type_='date')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('column1 DATE', query)

    def test_converts_datetime_to_postgresql_type(self):
        mprows = _get_fake_partition(type_='datetime')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('column1 TIMESTAMP WITHOUT TIME ZONE', query)

    def test_return_foreign_table_create_query(self):
        mprows = _get_fake_partition(type_='str')
        query = _get_create_query(mprows, 'vid1')
        self.assertIn('CREATE FOREIGN TABLE', query)


class MPRForeignDataWrapperTest(TestBase):

    def test_raises_RuntimeError_if_path_not_given(self):
        options = {}
        columns = []
        try:
            MPRForeignDataWrapper(options, columns)
            raise AssertionError('RuntimeError was not raised.')
        except RuntimeError as exc:
            self.assertIn('`path` is required option', str(exc))

    def test_raises_RuntimeError_if_filesystem_is_not_given(self):
        options = {'path': '/tmp'}
        columns = []
        try:
            MPRForeignDataWrapper(options, columns)
            raise AssertionError('RuntimeError was not raised.')
        except RuntimeError as exc:
            self.assertIn('`filesystem` is required option', str(exc))

    # _matches tests
    @patch('ambry_sources.med.postgresql.log_to_postgres')
    def test_adds_warning_message_with_missed_operator_to_postgres_log(self, fake_log):
        options = {
            'path': 'file1.mpr',  # These are not valid path and filesystem. But it does not matter here.
            'filesystem': '/tmp'}
        columns = []
        mpr_wrapper = MPRForeignDataWrapper(options, columns)
        fake_qual = AttrDict({'operator': 'foo'})
        mpr_wrapper._matches([fake_qual], ['1'])
        self.assertEqual(fake_log.call_count, 1)
        fake_log.assert_called_with(
            'Unknown operator foo in the AttrDict({\'operator\': \'foo\'}) qual. Row will be returned.',
            WARNING,
            hint='Implement foo operator in the MPR FDW wrapper.')

    def test_returns_true_if_row_matches_all_quals(self):
        options = {
            'path': 'file1.mpr',  # These are not valid path and filesystem. But it does not matter here.
            'filesystem': '/tmp'}
        columns = ['column1']
        mpr_wrapper = MPRForeignDataWrapper(options, columns)
        fake_qual = AttrDict({
            'operator': '=',
            'field_name': 'column1',
            'value': '1'})
        self.assertTrue(mpr_wrapper._matches([fake_qual], ['1']))

    def test_returns_false_if_row_does_not_match_all_quals(self):
        options = {
            'path': 'file1.mpr',  # These are not valid path and filesystem. But it does not matter here.
            'filesystem': '/tmp'}
        columns = ['column1']
        mpr_wrapper = MPRForeignDataWrapper(options, columns)
        fake_qual = AttrDict({
            'operator': '=',
            'field_name': 'column1',
            'value': '1'})
        self.assertFalse(mpr_wrapper._matches([fake_qual], ['2']))

    # _execute tests
    def test_generates_rows_from_message_pack_rows_file(self):
        options = {
            'path': 'file1.mpr',  # These are not valid path and filesystem. But it does not matter here.
            'filesystem': '/tmp'}
        columns = []
        mpr_wrapper = MPRForeignDataWrapper(options, columns)

        class FakeReader(object):
            rows = iter([['1-1', '1-2'], ['2-1', '2-2']])

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with patch.object(MPRowsFile, 'reader', FakeReader()):
            rows_itr = mpr_wrapper.execute([], ['column1', 'column2'])
            rows = list(rows_itr)
            expected_rows = [
                ['1-1', '1-2'],
                ['2-1', '2-2']]

        self.assertEqual(rows, expected_rows)


def _get_fake_partition(type_='str'):
    class FakePartition(object):
        reader = AttrDict({'columns': [{'type': type_, 'name': 'column1', 'pos': 0}]})
        path = 'name1'
        _fs = AttrDict({'root_path': '/tmp'})
    return FakePartition()
