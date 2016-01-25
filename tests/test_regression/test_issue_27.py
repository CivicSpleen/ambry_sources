# -*- coding: utf-8 -*-

from unittest import TestCase

from ambry_sources.intuit import RowIntuiter

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/27 issue.


class Test(TestCase):

    def test_converts_tuples(self):
        ret = RowIntuiter.coalesce_headers([
            ('Header-row0', ''),
            ('Header-row1', ''),
            ('Header-row2-1', 'Header-row2-2')])
        self.assertEqual(len(ret), 2)
        self.assertEqual(ret[0], 'Header-row0 Header-row1 Header-row2-1')
        self.assertEqual(ret[1], 'Header-row0 Header-row1 Header-row2-2')
