# -*- coding: utf-8 -*-

import apsw

from fs.opener import fsopendir

from ambry_sources.med.sqlite import add_partition, table_name
from ambry_sources.mpf import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from tests import TestBase

# https://github.com/CivicKnowledge/ambry_sources/issues/21


class Test(TestBase):

    def _get_generator_source(self, header, rows):

        def gen():
            # generate header
            yield header

            # generate rows
            for row in rows:
                yield row

        return GeneratorSource(SourceSpec('foobar'), gen())

    def test_selects_correct_rows_from_many_mprows(self):

        fs = fsopendir('temp://')
        header = ['col1', 'col2']

        # create 3 mprows files.
        #
        rows1 = [(0, 0), (1, 1)]
        mprows1 = MPRowsFile(fs, 'vid1')
        mprows1.load_rows(self._get_generator_source(header, rows1))

        rows2 = [(2, 2), (3, 3)]
        mprows2 = MPRowsFile(fs, 'vid2')
        mprows2.load_rows(self._get_generator_source(header, rows2))

        rows3 = [(4, 4), (5, 5)]
        mprows3 = MPRowsFile(fs, 'vid3')
        mprows3.load_rows(self._get_generator_source(header, rows3))

        # create virtual tables for all mprows
        #
        connection = apsw.Connection(':memory:')

        add_partition(connection, mprows1, 'vid1')
        add_partition(connection, mprows2, 'vid2')
        add_partition(connection, mprows3, 'vid3')

        # check rows of all added mprows.
        #

        cursor = connection.cursor()
        query_tmpl = 'SELECT * FROM {};'

        # check rows of the first file.
        #
        query = query_tmpl.format(table_name('vid1'))
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, rows1)

        # check rows of the second mprows file.
        #
        query = query_tmpl.format(table_name('vid2'))
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, rows2)

        # check rows of the third mprows file.
        #
        query = query_tmpl.format(table_name('vid3'))
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, rows3)
