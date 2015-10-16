# -*- coding: utf-8 -*-

from fs.opener import fsopendir

try:
    # py3
    from unittest.mock import MagicMock, patch, call
except ImportError:
    # py2
    from mock import MagicMock, patch, call

from ambry_sources.hdf_partitions.core import HDFWriter, HDFPartition

from tests import TestBase


class TestHDFWriter(TestBase):

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
    def test_writes_given_rows_to_h5_file(self):
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

    def test_writes_cached_rows_to_h5_file(self):
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

    # write_meta tests
    def test_writes_meta_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')

    def test_writes_meta_about_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'about')
        # FIXME: Check saved values.

    def test_writes_meta_comments_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'comments')
        # FIXME: Check saved values.

    def test_writes_meta_excel_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'excel')
        # FIXME: Check saved values.

    def test_writes_meta_geo_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'geo')
        # FIXME: Check saved values.

    def test_writes_meta_row_spec_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'row_spec')
        # FIXME: Check saved values.

    def test_writes_meta_schema_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'schema')
        # FIXME: Check saved values.

    def test_writes_meta_source_to_h5_file(self):
        temp_fs = fsopendir('temp://')
        parent = MagicMock()
        writer = HDFWriter(parent, temp_fs.getsyspath('temp.h5'))
        writer.write_meta()

        self.assertEqual(writer.cache, [])
        self.assertTrue(writer._h5_file.root.partition, 'meta')
        self.assertTrue(writer._h5_file.root.partition.meta, 'source')
        # FIXME: Check saved values.
