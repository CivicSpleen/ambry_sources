# -*- coding: utf-8 -*-
import unittest
from collections import OrderedDict

import fudge
from fudge.inspector import arg

from ambry_sources.sources import SourceSpec, ShapefileSource


class TestShapefileSource(unittest.TestCase):

    def _get_fake_collection(self):
        """ Returns fake collection which can be used as replaced for fiona.open(...) return value. """

        class FakeCollection(object):
            schema = {
                'properties': OrderedDict([('col1', 'int:10')])}

            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                pass

            def __iter__(self):
                return iter([{'properties': OrderedDict([('col1', 1)]), 'geometry': 'LINE', 'id': '0'}])

        return FakeCollection()

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

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_reads_first_layer_if_spec_segment_is_empty(self, fake_open, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().with_args(arg.any(), vfs=arg.any(), layer=0).returns(fake_collection)
        fake_shape.is_a_stub()
        fake_dumps.is_a_stub()

        spec = SourceSpec('http://example.com')
        assert spec.segment is None
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_reads_layer_specified_by_segment(self, fake_open, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().with_args(arg.any(), vfs=arg.any(), layer=5).returns(fake_collection)
        fake_shape.is_a_stub()
        fake_dumps.is_a_stub()
        spec = SourceSpec('http://example.com', segment=5)
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.ShapefileSource._get_columns',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_populates_columns_of_the_spec(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().returns(fake_collection)
        fake_get.expects_call().returns([{'name': 'col1', 'type': 'int'}])
        fake_shape.is_a_stub()
        fake_dumps.is_a_stub()
        spec = SourceSpec('http://example.com')
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEquals(spec.columns, [{'name': 'col1', 'type': 'int'}])

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.ShapefileSource._get_columns',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_converts_row_id_to_integer(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().returns(fake_collection)
        fake_shape.expects_call().is_a_stub()
        fake_dumps.expects_call().is_a_stub()
        fake_get.expects_call().returns([{'name': 'col1', 'type': 'int'}])
        spec = SourceSpec('http://example.com')
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        row_gen = source._get_row_gen()
        first_row = next(row_gen)
        self.assertEqual(first_row[0], 0)

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.ShapefileSource._get_columns',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_saves_header(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().returns(fake_collection)
        fake_get.expects_call().returns([
            {'name': 'id', 'type': 'int'},
            {'name': 'col1', 'type': 'int'},
            {'name': 'geometry', 'type': 'geometry_type'}])
        fake_shape.expects_call().is_a_stub()
        fake_dumps.expects_call().is_a_stub()
        spec = SourceSpec('http://example.com')
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        next(source._get_row_gen())
        self.assertEqual(source._headers, ['id', 'col1', 'geometry'])

    @fudge.patch(
        'ambry_sources.sources.accessors.fiona.open',
        'ambry_sources.sources.accessors.ShapefileSource._get_columns',
        'ambry_sources.sources.accessors.shape',
        'ambry_sources.sources.accessors.dumps')
    def test_last_element_in_the_row_is_wkt(self, fake_open, fake_get, fake_shape, fake_dumps):
        fake_collection = self._get_fake_collection()
        fake_open.expects_call().returns(fake_collection)
        fake_shape.expects_call().is_a_stub()
        fake_dumps.expects_call().returns('I AM FAKE WKT')
        fake_get.expects_call().returns([{'name': 'col1', 'type': 'int'}])
        spec = SourceSpec('http://example.com')
        fstor = fudge.Fake().is_a_stub()
        source = ShapefileSource(spec, fstor)
        row_gen = source._get_row_gen()
        first_row = next(row_gen)
        self.assertEqual(first_row[-1], 'I AM FAKE WKT')
