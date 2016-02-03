# -*- coding: utf-8 -*-

import unittest

from fs.opener import fsopendir

import pytest

import six
from six import u

from ambry_sources import get_source
from ambry_sources.mpf import MPRowsFile

from tests import TestBase


class BasicTestSuite(TestBase):
    """Basic test cases."""

    @classmethod
    def setUpClass(cls):
        super(BasicTestSuite, cls).setUpClass()
        cls.sources = cls.load_sources()

    @unittest.skip('Useful for debugging, but doesnt add test coverage')
    def test_just_download(self):
        """Just check that all of the sources can be downloaded without exceptions"""

        cache_fs = fsopendir('temp://')

        for source_name, spec in self.sources.items():
            try:
                s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

                for i, row in enumerate(s):
                    if i > 10:
                        break
            except Exception as exc:
                raise AssertionError('Failed to download {} source because of {} error.'
                                     .format(s.url, exc))

    @pytest.mark.slow
    def test_load_check_headers(self):
        """Just check that all of the sources can be loaded without exceptions"""

        cache_fs = fsopendir('temp://')

        headers = {
            'mz_with_zip_xl': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')],
            'mz_no_zip': [u('id'), u('uuid'), u('int'), u('float')],
            'namesu8': [u('origin_english'), u('name_english'), u('origin_native'), u('name_native')],
            'sf_zip': [u('id'), u('uuid'), u('int'), u('float')],
            'simple': [u('id'), u('uuid'), u('int'), u('float')],
            'csv_no_csv': [u('id'), u('uuid'), u('int'), u('float')],
            'mz_with_zip': [u('id'), u('uuid'), u('int'), u('float')],
            'rpeople': [u('name'), u('size')],
            'rent07': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')],
            'simple_fixed': [u('id'), u('uuid'), u('int'), u('float')],
            'altname': [u('id'), u('foo'), u('bar'), u('baz')],
            'rentcsv': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')],
            'renttab': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')],
            'multiexcel': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')],
            'rent97': [
                u('id'), u('gvid'), u('renter_cost_gt_30'), u('renter_cost_gt_30_cv'),
                u('owner_cost_gt_30_pct'), u('owner_cost_gt_30_pct_cv')]
        }

        for source_name, spec in self.sources.items():
            print(source_name)
            s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

            f = MPRowsFile(cache_fs, spec.name)

            if f.exists:
                f.remove()

            f.load_rows(s)

            with f.reader as r:
                if spec.name in headers:
                    self.assertEqual(headers[spec.name], r.headers)

    @unittest.skip('Useful for debugging, but doesnt add test coverage')
    @pytest.mark.slow
    def test_full_load(self):
        """Just check that all of the sources can be loaded without exceptions"""

        cache_fs = fsopendir('temp://')

        for source_name, spec in self.sources.items():

            s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

            print(spec.name)

            f = MPRowsFile(cache_fs, spec.name)

            if f.exists:
                f.remove()

            f.load_rows(s)

            with f.reader as r:
                self.assertTrue(len(r.headers) > 0)

    def test_fixed(self):
        cache_fs = fsopendir(self.setup_temp_dir())
        spec = self.sources['simple_fixed']
        s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))
        f = MPRowsFile(cache_fs, spec.name).load_rows(s)
        self.assertEqual(f.headers, ['id', 'uuid', 'int', 'float'])

    def test_generator(self):
        from ambry_sources.sources import GeneratorSource, SourceSpec

        cache_fs = fsopendir(self.setup_temp_dir())

        def gen():

            yield list('abcde')

            for i in range(10):
                yield [i, i+1, i+2, i+3, i+4]

        f = MPRowsFile(cache_fs, 'foobar').load_rows(GeneratorSource(SourceSpec('foobar'), gen()))

        self.assertEqual(1,  f.info['data_start_row'])
        self.assertEqual(11, f.info['data_end_row'])
        self.assertEqual([0],  f.info['header_rows'])

        self.assertEqual(f.headers, list('abcde'))
        rows = list(f.select())
        self.assertEqual(len(rows), 10)
        self.assertEqual(sorted(rows[0].keys()), sorted(list('abcde')))

        self.assertTrue(f.is_finalized)

    def test_type_intuit(self):
        from ambry_sources.intuit import TypeIntuiter

        cache_fs = fsopendir(self.setup_temp_dir())
        spec = self.sources['simple_fixed']
        s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

        f = MPRowsFile(cache_fs, spec.name)

        with f.writer as w:
            w.load_rows(s)

        with f.reader as r:
            ti = TypeIntuiter().process_header(r.headers).run(r.rows, r.n_rows)

        with f.writer as w:
            w.set_types(ti)

        with f.reader as w:
            for col in w.columns:
                print(col.pos, col.name, col.type)

    @pytest.mark.slow
    def test_row_intuit(self):
        """Check that the sources can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        from ambry_sources.intuit import RowIntuiter

        cache_fs = fsopendir('temp://')
        # cache_fs = fsopendir('/tmp/ritest/')

        sources = self.load_sources('sources-non-std-headers.csv')

        for source_name, spec in sources.items():

            s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

            rows = list(s)
            l = len(rows)

            # the files are short, so the head and tail overlap
            ri = RowIntuiter(debug=False).run(rows[:int(l*.75)], rows[int(l*.25):], len(rows))

            print source_name, ri.start_line, ri.header_lines

            self.assertEqual(
                spec.expect_headers,
                ','.join(str(e) for e in ri.header_lines),
                'Headers of {} source does not match to row intuiter'.format(spec.name))

            self.assertEqual(
                spec.expect_start, ri.start_line,
                'Start line of {} source does not match to row intuiter start line.'.format(spec.name))

    @pytest.mark.slow
    def test_row_load_intuit(self):
        """Check that the sources can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        from itertools import islice

        cache_fs = fsopendir('temp://')
        cache_fs.makedir('/mpr')
        # cache_fs = fsopendir('/tmp/ritest/')

        sources = self.load_sources('sources-non-std-headers.csv')

        for source_name, spec in sources.items():


            s = get_source(spec, cache_fs, callback=lambda x, y: (x, y))

            f = MPRowsFile(cache_fs, '/mpr/'+source_name)

            if f.exists:
                f.remove()

            print "Loading ", source_name, spec.url
            f.load_rows(s, intuit_type=False, run_stats=False, limit=500)

            self.assertEqual(f.info['data_start_row'], spec.expect_start)

            with f.reader as r:
                # First row, marked with metadata, that is marked as a data row
                m1, row1 = next(six.moves.filter(lambda e: e[0][2] == 'D', r.meta_raw))

            with f.reader as r:
                # First row
                row2 = next(r.rows)

            with f.reader as r:
                # First row proxy
                row3 = next(iter(r)).row

            self.assertEqual(row1, row2)
            self.assertEqual(row1, row3)

            with f.reader as r:
                raw_rows = list(islice(r.raw, None, 40))

            self.assertEqual(row2, raw_rows[f.info['data_start_row']])

    def test_headers(self):

        fs = fsopendir('mem://')

        df = MPRowsFile(fs, 'foobar')

        with df.writer as w:

            schema = lambda row, col: w.meta['schema'][row][col]

            w.headers = list('abcdefghi')

            self.assertEqual('a', schema(1, 1))
            self.assertEqual('e', schema(5, 1))
            self.assertEqual('i', schema(9, 1))

            for h in w.columns:
                h.description = "{}-{}".format(h.pos, h.name)

            self.assertEqual('1-a', schema(1, 3))
            self.assertEqual('5-e', schema(5, 3))
            self.assertEqual('9-i', schema(9, 3))

            w.column(1).description = 'one'
            w.column(2).description = 'two'
            w.column('c').description = 'C'
            w.column('d')['description'] = 'D'

            self.assertEqual('one', schema(1, 3))
            self.assertEqual('two', schema(2, 3))
            self.assertEqual('C', schema(3, 3))
            self.assertEqual('D', schema(4, 3))

        with df.reader as r:
            schema = lambda row, col: r.meta['schema'][row][col]

            self.assertEqual(
                [u('a'), u('b'), u('c'), u('d'), u('e'), u('f'), u('g'), u('h'), u('i')],
                r.headers)

            self.assertEqual('one', schema(1, 3))
            self.assertEqual('two', schema(2, 3))
            self.assertEqual('C', schema(3, 3))
            self.assertEqual('D', schema(4, 3))

    def test_intuit_footer(self):
        sources = self.load_sources(file_name='sources.csv')

        for source_name in ['headers4', 'headers3', 'headers2', 'headers1']:
            cache_fs = fsopendir(self.setup_temp_dir())

            spec = sources[source_name]
            f = MPRowsFile(cache_fs, spec.name)\
                .load_rows(get_source(spec, cache_fs, callback=lambda x, y: (x, y)))

            with f.reader as r:
                last = list(r.rows)[-1]  # islice isn't working on the reader.
                print source_name, last
                self.assertEqual(11999, int(last[0]))
                self.assertEqual('2q080z003Cg2', last[1])

    def test_intuit_headers(self):
        sources = self.load_sources(file_name='sources.csv')

        for source_name in ['headers4', 'headers3', 'headers2', 'headers1']:
            cache_fs = fsopendir(self.setup_temp_dir())

            spec = sources[source_name]
            # print '-----', source_name
            f = MPRowsFile(cache_fs, spec.name)\
                .load_rows(get_source(spec, cache_fs, callback=lambda x, y: (x, y)))

            self.assertEqual(spec.expect_start, f.info['data_start_row'])
            self.assertEquals([int(e) for e in spec.expect_headers.split(',')], f.info['header_rows'])

    def test_header_coalesce(self):
        from ambry_sources.intuit import RowIntuiter


        def csplit(h):
            return [ r.split(',') for r in h]

        h = [
            "a1,,a3,,a5,,a7",
            "b1,,b3,,b5,,b7",
            ",c2,,c4,,c6,",
            "d1,d2,d3,d4,d5,d6,d7"
        ]

        hc = [u'a1 b1 d1',
              u'a1 b1 c2 d2',
              u'a3 b3 c2 d3',
              u'a3 b3 c4 d4',
              u'a5 b5 c4 d5',
              u'a5 b5 c6 d6',
              u'a7 b7 c6 d7']


        self.assertEqual(hc, RowIntuiter.coalesce_headers(csplit(h)))


    @pytest.mark.slow
    def test_datafile_read_write(self):
        from fs.opener import fsopendir
        import datetime
        from random import randint, random
        from contexttimer import Timer
        from uuid import uuid4

        fs = fsopendir('mem://')

        # fs = fsopendir('/tmp/pmpf')

        N = 50000

        # Basic read/ write tests.

        def rand_date():
            return datetime.date(randint(2000, 2015), randint(1, 12), 10)

        def rand_datetime():
            return datetime.datetime(randint(2000, 2015), randint(1, 12), 10)

        def rand_time():
            return datetime.time(randint(0, 23), randint(0, 59), 10)

        row = lambda: (None, 1, random(), str(uuid4()), rand_date(), rand_datetime(), rand_time())

        headers = list('abcdefghi')[:len(row())]

        rows = [row() for i in range(N)]

        def write_large_blocks():

            df = MPRowsFile(fs, 'foobar')

            if df.exists:
                df.remove()

            with Timer() as t, df.writer as w:
                w.headers = headers
                w.insert_rows(rows)

            print('MSGPack write L', float(N) / t.elapsed, w.n_rows)

        def write_small_blocks():
            df = MPRowsFile(fs, 'foobar')

            if df.exists:
                df.remove()

            with Timer() as t, df.writer as w:

                for i in range(N):
                    w.headers = headers
                    w.insert_row(rows[i])

            print('MSGPack write S', float(N) / t.elapsed, w.n_rows)

        print()
        # Write the whole file with insert_rows() which writes all of the rows at once.
        write_large_blocks()

        # Write the file in blocks, with insert_rows collecting rows into a cache, then writting the
        # cached blocks.
        write_small_blocks()

        df = MPRowsFile(fs, 'foobar')

        with Timer() as t:
            count = 0
            i = 0
            s = 0

            r = df.reader

            for i, row in enumerate(r):
                count += 1
            r.close()

        print('MSGPack read   ', float(N) / t.elapsed, i, count, s)

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.rows:

                count += 1

            r.close()

        print('MSGPack rows   ', float(N) / t.elapsed)

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.raw:
                count += 1

            r.close()

        print('MSGPack raw    ', float(N) / t.elapsed)

    def generate_rows(self, N):
        import datetime
        import string

        rs = string.ascii_letters

        row = lambda x: [x, x * 2, x % 17, rs[x % 19:x % 19 + 20],
                         datetime.date(2000 + x % 15, 1 + x % 12, 10),
                         datetime.date(2000 + (x + 1) % 15, 1 + (x + 1) % 12, 10)]

        headers = list('abcdefghi')[:len(row(0))]

        rows = [row(i) for i in range(1, N+1)]

        return rows, headers

    def test_stats(self):
        """Check that the sources can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        #cache_fs = fsopendir('temp://')
        from shutil import rmtree
        from os import makedirs

        tp = '/tmp/mpr-test'
        rmtree(tp, ignore_errors=True)
        makedirs(tp)
        cache_fs = fsopendir(tp)

        s = get_source(self.sources['simple_stats'], cache_fs, callback=lambda x, y: (x, y))

        f = MPRowsFile(cache_fs, s.spec.name).load_rows(s, run_stats=True)

        print 'File saved to ', f.syspath

        stat_names = ('count','min','mean','max','nuniques')

        vals = {u('str_a'):   (30, None, None, None, 10),
                u('str_b'):   (30, None, None, None, 10),
                u('float_a'): (30, 1.0, 5.5, 10.0, 10),
                u('float_b'): (30, 1.1, 5.5, 9.9, 10),
                u('float_c'): (30, None, None, None, 10),
                u('int_b'):   (30, None, None, None, 10),
                u('int_a'):   (30, 1.0, 5.5, 10.0, 10)}

        with f.reader as r:

            for col in r.columns:
                stats = (col.stat_count,
                         col.min,
                         round(col.mean, 1) if col.mean else None,
                         col.max,
                         col.nuniques)



                for a, b, stat_name in zip(vals[col.name], stats, stat_names):
                    self.assertEqual(a, b, "{} failed for stat {}: {} != {}".format(col.name, stat_name, a, b))

    def test_datafile(self):
        """
        Test Loading and interating over data files, exercising the three header cases, and the use
        of data start and end lines.

        :return:
        """
        from itertools import islice

        N = 500

        rows, headers = self.generate_rows(N)

        def first_row_header(data_start_row=None, data_end_row=None):

            # Normal Headers
            f = MPRowsFile('mem://frh')
            w = f.writer

            w.columns = headers

            for row in rows:
                w.insert_row(row)

            if data_start_row is not None:
                w.data_start_row = data_start_row

            if data_end_row is not None:
                w.data_end_row = data_end_row

            w.close()

            self.assertEqual(
                (u('a'), u('b'), u('c'), u('d'), u('e'), u('f')),
                tuple(w.parent.reader.headers))

            w.parent.reader.close()

            return f

        def no_header(data_start_row=None, data_end_row=None):

            # No header, column labels.
            f = MPRowsFile('mem://nh')
            w = f.writer

            for row in rows:
                w.insert_row(row)

            if data_start_row is not None:
                w.data_start_row = data_start_row

            if data_end_row is not None:
                w.data_end_row = data_end_row

            w.close()

            self.assertEqual(['col1', 'col2', 'col3', 'col4', 'col5', 'col6'], w.parent.reader.headers)

            w.parent.reader.close()

            return f

        def schema_header(data_start_row=None, data_end_row=None):
            # Set the schema
            f = MPRowsFile('mem://sh')
            w = f.writer

            w.headers = ['x' + str(e) for e in range(len(headers))]

            for row in rows:
                w.insert_row(row)

            if data_start_row is not None:
                w.data_start_row = data_start_row

            if data_end_row is not None:
                w.data_end_row = data_end_row

            w.close()

            self.assertEqual(
                (u('x0'), u('x1'), u('x2'), u('x3'), u('x4'), u('x5')),
                tuple(w.parent.reader.headers))

            w.parent.reader.close()

            return f

        # Try a few header start / data start values.

        for ff in (first_row_header, schema_header, no_header):
            f = ff()

            with f.reader as r:
                l = list(r.rows)

                self.assertEqual(N, len(l))

            with f.reader as r:
                # Check that the first row value starts at one and goes up from there.
                map(lambda f: self.assertEqual(f[0], f[1][0]), enumerate(islice(r.rows, 5), 1))

        for ff in (first_row_header, schema_header, no_header):
            data_start_row = 5
            data_end_row = 15
            f = ff(data_start_row, data_end_row)

            with f.reader as r:
                l = list(r.rows)
                self.assertEqual(11, len(l))

    def test_spec_load(self):
        """Test that setting a SourceSpec propertly sets the header_lines data start position"""

        from ambry_sources.sources import SourceSpec
        import string

        rs = string.ascii_letters

        n = 500

        rows, headers = self.generate_rows(n)

        blank = ['' for e in rows[0]]

        # Append a complex header, to give the RowIntuiter something to do.
        rows = [
            ['Dataset Title'] + blank[1:],
            blank,
            blank,
            [rs[i] for i, e in enumerate(rows[0])],
            [rs[i+1] for i, e in enumerate(rows[0])],
            [rs[i+2] for i, e in enumerate(rows[0])],
        ] + rows

        f = MPRowsFile('mem://frh').load_rows(rows)

        d = f.info

        self.assertEqual(6, d['data_start_row'])
        self.assertEqual(506, d['data_end_row'])
        self.assertEqual([3, 4, 5], d['header_rows'])
        self.assertEqual(
            [u('a_b_c'), u('b_c_d'), u('c_d_e'), u('d_e_f'), u('e_f_g'), u('f_g_h')],
            d['headers'])

        class Rows(object):
            spec = SourceSpec(None, header_lines=(3, 4), start_line=5)

            def __iter__(self):
                return iter(rows)

        f = MPRowsFile('mem://frh').load_rows(Rows())

        d = f.info

        self.assertEqual(5, d['data_start_row'])
        self.assertEqual(506, d['data_end_row'])
        self.assertEqual([3, 4], d['header_rows'])
        self.assertEqual(
            [u('a_b'), u('b_c'), u('c_d'), u('d_e'), u('e_f'), u('f_g')],
            d['headers'])
