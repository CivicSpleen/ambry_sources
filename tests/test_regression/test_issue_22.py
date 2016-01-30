# -*- coding: utf-8 -*-

import apsw

from fs.opener import fsopendir

from ambry_sources.med.sqlite import add_partition, table_name
from ambry_sources.mpf import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from tests import TestBase

# https://github.com/CivicKnowledge/ambry_sources/issues/22


class Test(TestBase):

    def test_creates_virtual_tables_for_partition_with_segment_without_errors(self):

        fs = fsopendir('temp://')

        def gen():
            # generate header
            yield ['col1', 'col2']

            # generate rows
            yield [0, 0]
            yield [1, 1]

        mprows = MPRowsFile(fs, 'example.com/simple-0.1.3/1.mpr')
        mprows.load_rows(GeneratorSource(SourceSpec('foobar'), gen()))

        # create virtual tables. This should not raise an error.
        #
        connection = apsw.Connection(':memory:')
        try:
            add_partition(connection, mprows, 'vid1')
        except Exception as exc:
            raise AssertionError('partition adding unexpectadly failed with {} error.'.format(exc))

        # check selected rows
        #
        cursor = connection.cursor()
        result = cursor.execute('SELECT * FROM {}'.format(table_name('vid1'))).fetchall()
        self.assertEqual(result, [(0, 0), (1, 1)])
