# -*- coding: utf-8 -*-
import unittest

import fiona
import fudge

from ambry_sources.sources import SourceSpec, ShapefileSource


class TestShapefileSource(unittest.TestCase):

    def test_reads_first_layer_if_spec_segment_is_empty(self):
        # cache open of the fiona because we need to call it in mocked environment.
        self._layer_used = None

        class FakeError(Exception):
            pass

        def open_replacement(path, vfs=None, layer=None):
            self._layer_used = layer
            # Now I know everything I need to implement that test. Break right now.
            raise FakeError

        spec = SourceSpec('http://example.com')
        assert spec.segment is None
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        with fudge.patched_context(fiona, 'open', open_replacement):
            # we need to get at least one row to test used layer.
            try:
                next(source._get_row_gen())
            except FakeError:
                pass
        self.assertEqual(self._layer_used, 0)

    def test_reads_layer_specified_by_segment(self):
        # cache open of the fiona because we need to call it in mocked environment.
        self._layer_used = None

        class FakeError(Exception):
            pass

        def open_replacement(path, vfs=None, layer=None):
            self._layer_used = layer
            # Now I know everything I need for that test. Break right now.
            raise FakeError

        spec = SourceSpec('http://example.com', segment=5)
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        with fudge.patched_context(fiona, 'open', open_replacement):
            # we need to get at least one row to test used layer.
            try:
                next(source._get_row_gen())
            except FakeError:
                pass
        self.assertEqual(self._layer_used, 5)

    def test_header_populating(self):
        pass

    def test_first_element_is_id_of_the_shape(self):
        pass

    def test_middle_elements_are_columns_data(self):
        pass

    def test_last_element_is_wkt(self):
        pass
