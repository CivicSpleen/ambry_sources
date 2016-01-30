# -*- coding: utf-8 -*-

from ambry_sources import get_source

from fs.zipfs import ZipFS
from fs.opener import fsopendir

from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/6 issue.


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_created_source_has_zip_filesystem(self):
        # FIXME: Optimize to use local file instead of downloading it all the time.
        cache_fs = fsopendir(self.setup_temp_dir())
        sources = self.load_sources(file_name='geo_sources.csv')
        spec = sources['community_plan']
        source = get_source(spec, cache_fs, callback=lambda x, y: (x, y))
        self.assertIsInstance(source._fstor._fs, ZipFS)
