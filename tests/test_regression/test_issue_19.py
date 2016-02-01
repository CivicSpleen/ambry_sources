# -*- coding: utf-8 -*-

from ambry_sources import get_source

from fs.opener import fsopendir
from ambry_sources.mpf import MPRowsFile
from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/19


class Test(TestBase):
    """ Test footer intuition """

    def test_intuit_footer(self):

        sources = self.load_sources(file_name='sources.csv')

        for source_name in ['headers4', 'headers3', 'headers2', 'headers1']:

            cache_fs = fsopendir(self.setup_temp_dir())

            spec = sources[source_name]
            f = MPRowsFile(cache_fs, spec.name)\
                .load_rows(get_source(spec, cache_fs, callback=lambda x, y: (x, y)))

            with f.reader as r:
                last = list(r.rows)[-1]  # islice isn't working on the reader.
                self.assertEqual(11999, int(last[0]))
                self.assertEqual('2q080z003Cg2', last[1])
