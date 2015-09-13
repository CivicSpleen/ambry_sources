# -*- coding: utf-8 -*-

import apsw

from six import u

from ambry_sources.med.sqlite import add_partition, _table_name

from tests.test_med import BaseMEDTest, TEST_FILES_DIR

# Temporary tests. Will be removed later.
from fs.opener import fsopendir
from ambry_sources.sources import SourceSpec, CsvSource, DelayedOpen
from ambry_sources.mpf import MPRowsFile


class Test(BaseMEDTest):

    def test_column_recoginized_as_int_loads_as_str(self):
        # see tests/test_med/files for appropriate files.
        spec = SourceSpec('http://example.com', expect_start='2', name='test', expect_headers='1')
        memory_fs = fsopendir('mem://')
        files_fs = fsopendir(TEST_FILES_DIR)
        file_name = 'col1_int_col2_str_100_rows.csv'
        fstor = DelayedOpen(files_fs, file_name, 'rb')
        source = CsvSource(spec, fstor)

        # load rows from source
        f = MPRowsFile(memory_fs, '/mpr/' + file_name + '.msg').load_rows(source)
        # check first column. It is recognized as int.
        self.assertEqual(f.schema[0]['type'], 'int')

        # read rows. Column with type int contains unicode.
        with f.reader as r:
            for row in r.rows:
                # I think row[0] has to be int here.
                self.assertIsInstance(row[0], unicode)

    def test_row_intuiter_invented_columns(self):
        # see tests/test_med/files for appropriate files.
        spec = SourceSpec('http://example.com', expect_start='2', name='test', expect_headers='1')
        memory_fs = fsopendir('mem://')
        files_fs = fsopendir(TEST_FILES_DIR)
        file_name = 'col1_date_col2_datetime_100_rows.csv'
        fstor = DelayedOpen(files_fs, file_name, 'rb')
        source = CsvSource(spec, fstor)

        # load rows from source
        f = MPRowsFile(memory_fs, '/mpr/' + file_name + '.msg').load_rows(source)
        names = [x['name'] for x in f.schema]

        # Columns are recognized with error. Csv has column1 and column2 columns.
        # See col1_date_col2_datetime_100_rows.csv
        self.assertEquals(names, ['col0', 'col1'])
