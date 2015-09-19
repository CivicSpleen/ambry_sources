# -*- coding: utf-8 -*-

from ambry_sources import get_source
from ambry_sources.mpf import MPRowsFile

from fs.opener import fsopendir
from tests import TestBase


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_highways(self):
        # FIXME: Optimize to use local file instead of downloading it all the time.
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        spec = sources['highways']
        source = get_source(spec, cache_fs)

        # first check is it converted properly.
        row_gen = source._get_row_gen()
        header_row = next(row_gen)

        # spec columns are properly populated
        self.assertEqual(len(spec.columns), 68)
        self.assertEqual(spec.columns[0]['name'], 'id')
        self.assertEqual(spec.columns[-1]['name'], 'geometry')

        # generates valid header
        self.assertEqual(len(header_row), 68)
        self.assertEqual(header_row[0], 'id')
        self.assertEqual(header_row[-1], 'geometry')

        # generates valid first row
        first_row = next(row_gen)
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row[0], 0)
        # last element is wkt.
        self.assertIn('LINESTRING', first_row[-1])

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
