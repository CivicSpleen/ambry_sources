# -*- coding: utf-8 -*-

from fs.opener import fsopendir
from tables import open_file, StringCol, Int64Col, Float64Col, BoolCol

from ambry_sources.sources.util import RowProxy

try:
    # py3
    from unittest.mock import MagicMock, patch, call
except ImportError:
    # py2
    from mock import MagicMock, patch, call

from ambry_sources.hdf_partitions.core import HDFWriter, HDFPartition, HDFReader

from tests import TestBase


class HDFWriterTest(TestBase):

    def _get_column(self, name, type_, predefined=None):
        if not predefined:
            predefined = {}

        col = []
        for el in HDFPartition.SCHEMA_TEMPLATE:
            if el == 'name':
                col.append(name)
            elif el == 'type':
                col.append(type_)
            else:
                col.append(predefined.get(el, ''))
        return col

    # _write_rows test
    def test_writes_given_rows(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        # add two columns
        writer.meta['schema'].append(self._get_column('col1', 'int'))
        writer.meta['schema'].append(self._get_column('col2', 'str'))
        writer._write_rows(
            rows=[[1, 'row1'], [2, 'row2']])

        # rows are written
        self.assertEqual(writer._h5_file.root.partition.rows.nrows, 2)
        self.assertEqual(
            [x['col1'] for x in writer._h5_file.root.partition.rows.iterrows()],
            [1, 2])
        self.assertEqual(
            [x['col2'] for x in writer._h5_file.root.partition.rows.iterrows()],
            ['row1', 'row2'])

    def test_writes_cached_rows(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        # add two columns
        writer.meta['schema'].append(self._get_column('col1', 'int'))
        writer.meta['schema'].append(self._get_column('col2', 'str'))
        writer.cache = [[1, 'row1'], [2, 'row2']]
        writer._write_rows()

        self.assertEqual(writer.cache, [])
        # rows are written
        self.assertEqual(writer._h5_file.root.partition.rows.nrows, 2)
        self.assertEqual(
            [x['col1'] for x in writer._h5_file.root.partition.rows.iterrows()],
            [1, 2])
        self.assertEqual(
            [x['col2'] for x in writer._h5_file.root.partition.rows.iterrows()],
            ['row1', 'row2'])

    # insert_row test
    @patch('ambry_sources.hdf_partitions.core.HDFWriter._write_rows')
    def test_inserts_row_to_the_cache(self, fake_write_rows):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))

        writer.insert_row(['row1'])
        self.assertEqual(writer.n_rows, 1)
        self.assertEqual(writer.cache, [['row1']])
        fake_write_rows.assert_not_called()

    @patch('ambry_sources.hdf_partitions.core.HDFWriter._write_rows')
    def test_writes_rows_is_cache_is_large(self, fake_write_rows):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.cache = [[] for i in range(10000)]
        writer.insert_row(['row1'])
        fake_write_rows.assert_called_once_with()

    # load_rows test
    @patch('ambry_sources.hdf_partitions.core.HDFWriter.insert_row')
    @patch('ambry_sources.hdf_partitions.core.HDFWriter._write_rows')
    def test_inserts_and_writes_all_rows_from_source(self, fake_write_rows, fake_insert):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))

        writer.load_rows([['row1'], ['row2']])
        fake_write_rows.assert_called_once_with()
        self.assertEqual(
            fake_insert.mock_calls, [call(['row1']), call(['row2'])])

    # close tests
    def test_writes_rows_and_closes_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        h5_file = writer._h5_file
        with patch.object(writer, '_write_rows') as fake_write:
            writer.close()
            fake_write.assert_called_once_with()
        self.assertIsNone(writer._h5_file)
        self.assertEqual(h5_file.isopen, 0)

    # write_meta tests
    def test_writes_meta(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')

    def test_writes_meta_about(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['about']['load_time'] = 123.12
        writer.meta['about']['create_time'] = 111.11
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'about')
        self.assertEqual(
            [(x['load_time'], x['create_time']) for x in writer._h5_file.root.partition.meta.about.iterrows()],
            [(123.12, 111.11)])

    def test_writes_meta_comments(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['comments']['header'] = 'header'
        writer.meta['comments']['footer'] = 'footer'
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'comments')
        self.assertEqual(
            [(x['header'], x['footer']) for x in writer._h5_file.root.partition.meta.comments.iterrows()],
            [('header', 'footer')])

    def test_writes_meta_excel(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['excel']['worksheet'] = 'sheet1'
        writer.meta['excel']['datemode'] = 'mode1'
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'excel')
        self.assertEqual(
            [(x['worksheet'], x['datemode']) for x in writer._h5_file.root.partition.meta.excel.iterrows()],
            [('sheet1', 'mode1')])

    def test_writes_meta_geo(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['geo']['srs'] = 11
        writer.meta['geo']['bb'] = 22
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'geo')
        self.assertEqual(
            [(x['srs'], x['bb']) for x in writer._h5_file.root.partition.meta.geo.iterrows()],
            [(11, 22)])

    def test_writes_meta_row_spec(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['row_spec']['data_pattern'] = 'pattern'
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'row_spec')
        self.assertEqual(
            [x['data_pattern'] for x in writer._h5_file.root.partition.meta.row_spec.iterrows()],
            ['pattern'])

    def test_writes_meta_schema(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'schema')
        # FIXME: Check saved values.

    def test_writes_meta_source(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.meta['source']['encoding'] = 'utf-8'
        writer.meta['source']['url'] = 'http://example.com'
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'source')
        self.assertEqual(
            [(x['encoding'], x['url']) for x in writer._h5_file.root.partition.meta.source.iterrows()],
            [('utf-8', 'http://example.com')])


class HDFReaderTest(TestBase):

    def _create_h5(self, filename):
        # save meta.about to the file.
        with open_file(filename, 'w') as h5:
            descriptor = {
                'field1': Float64Col(),
                'field2': Float64Col(),
                'field3': Float64Col()
            }
            h5.create_group('/', 'partition')
            h5.create_table('/partition', 'rows', descriptor)
            table = h5.root.partition.rows
            row = table.row
            for i in range(5):
                row['field1'] = float(i)
                row['field2'] = float(i)
                row['field2'] = float(i)
                row.append()
            table.flush()

    # _write_rows test
    def test_raises_ValueError_if_file_like_given(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        try:
            HDFReader(parent, temp_fs.open('temp.h5', 'w'))
            raise AssertionError('ValueError was not raised.')
        except ValueError:
            pass

    # meta tests
    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta')
    def test_reads_meta_if_cache_is_empty(self, fake_read):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        reader = HDFReader(parent, filename)
        reader.meta
        fake_read.assert_called_once_with()

    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta')
    def test_uses_cached_meta(self, fake_read):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        reader = HDFReader(parent, filename)
        reader._meta = {}
        reader.meta
        fake_read.assert_not_called()

    # _read_meta tests
    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta_child')
    def test_returns_default_template(self, fake_read):
        fake_read.return_value = {}
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        reader = HDFReader(parent, filename)
        ret = reader._read_meta()
        expected_keys = ['about', 'excel', 'row_spec', 'source', 'comments', 'geo', 'schema']
        self.assertEqual(sorted(expected_keys), sorted(ret.keys()))

    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta_child')
    def test_reads_meta_children(self, fake_read):
        fake_read.return_value = {}
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        reader = HDFReader(parent, temp_fs.getsyspath('temp.h5'))
        reader._read_meta()

        # _read_meta_child was called properly
        self.assertEqual(len(fake_read.mock_calls), 7)
        self.assertIn(call('about'), fake_read.mock_calls)
        self.assertIn(call('excel'), fake_read.mock_calls)
        self.assertIn(call('row_spec'), fake_read.mock_calls)
        self.assertIn(call('source'), fake_read.mock_calls)
        self.assertIn(call('comments'), fake_read.mock_calls)
        self.assertIn(call('geo'), fake_read.mock_calls)
        self.assertIn(call('schema'), fake_read.mock_calls)

    # _read_meta_child tests
    def test_reads_first_line_and_returns_dict(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()

        # save meta.about to the file.
        with open_file(temp_fs.getsyspath('temp.h5'), 'w') as h5:
            descriptor = {
                'load_time': Float64Col(),
                'create_time': Float64Col()
            }
            h5.create_group('/partition', 'meta', createparents=True)
            h5.create_table('/partition/meta', 'about', descriptor, 'meta.group')
            table = h5.root.partition.meta.about
            row = table.row
            row['load_time'] = 1.0
            row['create_time'] = 1.1
            row.append()
            table.flush()

        # now read it from file.
        reader = HDFReader(parent, temp_fs.getsyspath('temp.h5'))
        ret = reader._read_meta_child('about')
        self.assertIsInstance(ret, dict)
        self.assertIn('load_time', ret)
        self.assertEqual(ret['load_time'], 1.0)

        self.assertIn('create_time', ret)
        self.assertEqual(ret['create_time'], 1.1)

    # columns test
    def test_contains_columns_specifications(self):
        # FIXME:
        pass

    # headers test
    def test_contains_columns_names(self):
        # FIXME:
        pass

    # raw tests
    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta')
    def test_contains_all_rows(self, fake_read):
        fake_read.return_value = {}
        temp_fs = fsopendir('temp://')
        parent = MagicMock()

        # save meta.about to the file.
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        # now read it from file.
        reader = HDFReader(parent, filename)
        raw_iter = reader.raw
        first = raw_iter.next()
        self.assertEqual(first, [0.0, 0.0, 0.0])
        self.assertTrue(reader._in_iteration)
        rows = list(raw_iter)
        self.assertEqual(len(rows), 4)
        self.assertFalse(reader._in_iteration)

    # rows tests
    @patch('ambry_sources.hdf_partitions.core.HDFReader._read_meta')
    def test_generates_all_rows(self, fake_read):
        fake_read.return_value = {}
        temp_fs = fsopendir('temp://')
        parent = MagicMock()

        # save meta.about to the file.
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        # now read it from file.
        reader = HDFReader(parent, filename)
        rows_iter = reader.rows
        first = rows_iter.next()
        self.assertEqual(first, [0.0, 0.0, 0.0])
        self.assertTrue(reader._in_iteration)

        rows = list(rows_iter)
        self.assertEqual(len(rows), 4)
        self.assertFalse(reader._in_iteration)

    # __iter__ tests
    def test_generates_rows_as_RowProxy_instances(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        reader = HDFReader(parent, filename)
        with patch.object(HDFReader, 'headers', ['field1', 'field2', 'field3']):
            rows_iter = iter(reader)
            first = rows_iter.next()
            self.assertIsInstance(first, RowProxy)
            self.assertEqual(first.field1, 0.0)
            self.assertEqual(first.field2, 0.0)
            self.assertEqual(first.field3, 0.0)
            self.assertTrue(reader._in_iteration)

            rows = []
            for row in rows_iter:
                self.assertIsInstance(row, RowProxy)
                rows.append(row)
            self.assertEqual(len(rows), 4)
            self.assertFalse(reader._in_iteration)

    # __enter__, __exist__ tests
    def test_with(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        filename = temp_fs.getsyspath('temp.h5')
        self._create_h5(filename)

        # now read it from file.
        with HDFReader(parent, filename) as reader:
            rows = []
            for row in reader:
                rows.append(row)
            self.assertEqual(len(rows), 5)
