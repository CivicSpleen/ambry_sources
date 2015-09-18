# -*- coding: utf-8 -*-
import unittest
from collections import OrderedDict

import fiona
import fudge

from ambry_sources.sources import SourceSpec, ShapefileSource


class TestShapefileSource(unittest.TestCase):

    # _convert_column tests
    def test_converts_shapefile_column(self):
        spec = fudge.Fake().is_a_stub()
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        expected_column = {'name': 'name1', 'type': 'int'}
        self.assertEqual(
            source._convert_column((u'name1', 'int:3')),
            expected_column)

    # _get_columns tests
    def test_converts_given_columns(self):
        spec = fudge.Fake().is_a_stub()
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        column1 = ('name1', 'int:10')
        column2 = ('name2', 'str:10')
        converted_column1 = {'name': 'name1', 'type': 'int'}
        converted_column2 = {'name': 'name2', 'type': 'str'}
        shapefile_columns = OrderedDict([column1, column2])
        ret = source._get_columns(shapefile_columns)
        self.assertIn(converted_column1, ret)
        self.assertIn(converted_column2, ret)

    def test_extends_with_id_and_geometry(self):
        spec = fudge.Fake().is_a_stub()
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        shapefile_columns = OrderedDict()
        ret = source._get_columns(shapefile_columns)
        self.assertEqual(len(ret), 2)
        names = [x['name'] for x in ret]
        self.assertIn('id', names)
        self.assertIn('geometry', names)

        types = [x['type'] for x in ret]
        self.assertIn('geometry_type', types)

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

    def test_populates_columns_of_the_spec(self):
        # FIXME:
        pass

    def test_first_element_is_id_of_the_shape(self):
        # FIXME:
        pass

    def test_middle_elements_are_columns_data(self):
        # FIXME:
        pass

    def test_last_element_is_wkt(self):
        # FIXME:
        pass
