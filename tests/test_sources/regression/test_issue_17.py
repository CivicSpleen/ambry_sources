# -*- coding: utf-8 -*-
from ambry_sources.sources.spec import SourceSpec

from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/17 issue.


class Test(TestBase):

    def test_converts_header_lines_of_strings_without_errors(self):
        spec = SourceSpec(url='http://example.com', header_lines=['1', '2', '3'])
        self.assertEqual(spec.header_lines, [1, 2, 3])

    def test_converts_header_lines_of_ints_without_errors(self):
        spec = SourceSpec(url='http://example.com', header_lines=[1, 2, 3])
        self.assertEqual(spec.header_lines, [1, 2, 3])
