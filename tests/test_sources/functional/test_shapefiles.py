# -*- coding: utf-8 -*-

from ambry_sources import get_source
from ambry_sources.mpf import MPRowsFile

from fs.opener import fsopendir
import pytest

from tests import TestBase


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    @pytest.mark.slow
    def test_highways(self):
        # FIXME: Optimize to use local file instead of downloading it all the time.
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        spec = sources['highways']
        source = get_source(spec, cache_fs)

        # first check is it converted properly.
        row_gen = source._get_row_gen()
        first_row = next(row_gen)

        # generates valid first row
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row[0], 0)
        # last element is wkt.
        self.assertIn('LINESTRING', first_row[-1])

        # spec columns are properly populated
        self.assertEqual(len(spec.columns), 68)
        self.assertEqual(spec.columns[0]['name'], 'id')
        self.assertEqual(spec.columns[-1]['name'], 'geometry')

        # header is valid
        self.assertEqual(len(source._headers), 68)
        self.assertEqual(source._headers[0], 'id')
        self.assertEqual(source._headers[-1], 'geometry')

        # now check its load to MPRows
        mpr = MPRowsFile(cache_fs, spec.name).load_rows(source)

        # Are columns recognized properly?
        columns = [x['name'] for x in mpr.schema]
        self.assertIn('id', columns)
        self.assertIn('geometry', columns)
        self.assertIn('length', columns)  # column from shape file.

        # Is first row valid?
        first_row = next(iter(mpr.reader))
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row['id'], 0)
        self.assertIn('LINESTRING', first_row['geometry'])

    @pytest.mark.slow
    def test_all(self):
        """ Test all sources from geo_sources.csv """
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        for name, spec in sources.iteritems():
            if name == 'highways':
                # it is already tested. Skip.
                continue

            source = get_source(spec, cache_fs)

            # now check its load to MPRows
            mpr = MPRowsFile(cache_fs, spec.name).load_rows(source)

            # Are columns recognized properly?
            columns = [x['name'] for x in mpr.schema]
            self.assertIn('id', columns)
            self.assertIn('geometry', columns)

            # Is first row valid?
            first_row = next(iter(mpr.reader))
            self.assertEqual(len(columns), len(first_row))
