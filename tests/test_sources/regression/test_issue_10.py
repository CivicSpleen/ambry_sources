# -*- coding: utf-8 -*-

from ambry_sources import get_source

from fs.zipfs import ZipFS
from fs.opener import fsopendir

from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/10 issue.


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_bad_row_intuition(self):
        from ambry_sources.mpf import MPRowsFile
        from ambry_sources.sources.spec import SourceSpec

        cache_fs = fsopendir('temp://')

        spec = SourceSpec('http://public.source.civicknowledge.com/example.com/sources/simple-example.csv',
                        name='simple')

        s = get_source(spec, cache_fs)

        f = MPRowsFile(cache_fs, spec.name)

        if f.exists:
            f.remove()

        f.load_rows(s)

        self.assertEqual(10001, f.reader.info['data_end_row'])