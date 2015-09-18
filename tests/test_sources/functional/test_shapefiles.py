# -*- coding: utf-8 -*-

from ambry_sources import get_source

from fs.opener import fsopendir
from tests import TestBase


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_highways(self):
        # FIXME: Optimize to use local file instead of downloading it all the time.
        cache_fs = fsopendir(self.setup_temp_dir())

        sources = self.load_sources(file_name='geo_sources.csv')
        spec = sources['highways']
        s = get_source(spec, cache_fs)
        row_gen = s._get_row_gen()
        first_row = next(row_gen)

        # spec columns are properly populated
        self.assertEqual(len(spec.columns), 68)
        self.assertEqual(spec.columns[0]['name'], 'id')
        self.assertEqual(spec.columns[-1]['name'], 'geometry')

        # generated row is valid
        self.assertEqual(len(first_row), 68)
        self.assertEqual(first_row[0], 'idFIXME:')
        # last element is wkt.
        self.assertIn('LINESTRING', first_row[-1])
