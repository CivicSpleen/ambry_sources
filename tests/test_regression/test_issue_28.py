# -*- coding: utf-8 -*-
from unittest import TestCase

from ambry_sources.sources.accessors import FixedSource
from ambry_sources.sources.util import DelayedOpen

from fs.opener import fsopendir

from ambry_sources.sources import SourceSpec, ColumnSpec

# https://github.com/CivicKnowledge/ambry_sources/issues/28


class Test(TestCase):

    def test_allow_source_data_to_start_from_0_row(self):
        columns = [
            ColumnSpec(name='col1', position=1, start=1, width=1),
            ColumnSpec(name='col2', position=2, start=3, width=1)]
        spec = SourceSpec('http://example.com', header_lines=[], start_line=0, columns=columns)
        assert spec.start_line == 0
        fs = fsopendir('temp://')
        with fs.open('temp.txt', 'w') as f:
            f.write(u'1 1\n')
            f.write(u'2 2\n')
            f.write(u'3 3\n')
        fstor = DelayedOpen(fs, 'temp.txt')

        fixed_source = FixedSource(spec, fstor)
        source_data = [x for x in fixed_source]
        self.assertEqual(len(source_data), 3)
        self.assertEqual(source_data, [['1', '1'], ['2', '2'], ['3', '3']])
