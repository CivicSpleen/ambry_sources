# -*- coding: utf-8 -*-


import unittest
import ambry_sources
from fs.opener import fsopendir

class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    def get_header_test_file(self, file_name):
        """ Creates source pipe from xls with given file name and returns it."""
        import os.path
        import tests
        import xlrd

        test_files_dir = os.path.join(os.path.dirname(tests.__file__), 'test_data', 'crazy_headers')

        class XlsSource(object):
            def __iter__(self):
                book = xlrd.open_workbook(os.path.join(test_files_dir, file_name))
                sheet = book.sheet_by_index(0)
                num_cols = sheet.ncols
                for row_idx in range(0, sheet.nrows):
                    row = []
                    for col_idx in range(0, num_cols):
                        value = sheet.cell(row_idx, col_idx).value
                        if value == '':
                            # FIXME: Is it valid requirement?
                            # intuiter requires None's in the empty cells.
                            value = None
                        row.append(value)
                    yield row

        return XlsSource()

    def load_sources(self, file_name = 'sources.csv'):
        import tests
        import csv
        from os.path import join, dirname
        from ambry_sources.sources import ColumnSpec, SourceSpec

        test_data = fsopendir(join(dirname(tests.__file__), 'test_data'))

        sources = {}

        fixed_widths = (('id', 1, 6),
                        ('uuid', 7, 34),
                        ('int', 41, 3),
                        ('float', 44, 14),
                        )

        fw_columns = [ColumnSpec(**dict(zip('name start width'.split(), e))) for e in fixed_widths]

        with test_data.open(file_name) as f:
            r = csv.DictReader(f)

            for row in r:

                if row['name'] == 'simple_fixed':
                    row['columns'] = fw_columns

                ss = SourceSpec(**row)

                if 'expect_headers' in row:
                    ss.expect_headers = row['expect_headers']
                    ss.expect_start = int(row['expect_start'])

                sources[ss.name] = ss

        return sources

    def test_download(self):
        """Just check that all of the sources can be downloaded without exceptions"""

        from ambry_sources import get_source

        cache_fs = fsopendir('temp://')

        sources = self.load_sources()

        for source_name, spec in sources.items():
            s = get_source(spec, cache_fs)
            print spec.url

            for i, row in enumerate(s):
                if i > 10:
                    break

    def test_row_intuit(self):
        """Check that the soruces can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        from ambry_sources import get_source
        from ambry_sources.intuit import RowIntuiter

        cache_fs = fsopendir('temp://')
        #cache_fs = fsopendir('/tmp/ritest/')

        sources = self.load_sources('sources-non-std-headers.csv')

        for source_name, spec in sources.items():
            s = get_source(spec, cache_fs)

            #if source_name != 'birth_profiles': continue

            print spec.name, spec.url

            ri = RowIntuiter().run(s)

            print ri.header_lines, ri.start_line

            self.assertEqual(spec.expect_headers,','.join(str(e) for e in ri.header_lines) )
            self.assertEqual(spec.expect_start, ri.start_line)

    def test_row_load_intuit(self):
        """Check that the soruces can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        from ambry_sources import get_source

        from ambry_sources.mpf import MPRowsFile
        from itertools import islice, ifilter

        cache_fs = fsopendir('temp://')
        cache_fs.makedir('/mpr')
        #cache_fs = fsopendir('/tmp/ritest/')

        sources = self.load_sources('sources-non-std-headers.csv')

        for source_name, spec in sources.items():

            s = get_source(spec, cache_fs)

            print source_name

            f = MPRowsFile(cache_fs, '/mpr/'+source_name).load_rows(s)

            with f.reader as r:
                # First row, marked with metadata, that is marked as a data row
                m1, row1 = next(ifilter(lambda e: e[0][2] == 'D', r.meta_raw))

            with f.reader as r:
                # First row
                row2 = next(r.rows)

            with f.reader as r:
                # First row proxy
                row3 = next(iter(r)).row

            self.assertEquals(row1, row2)
            self.assertEquals(row1, row3)

    def test_datafile_read_write(self):
        from ambry_sources.mpf import MPRowsFile
        from fs.opener import fsopendir
        import time
        import datetime
        from random import randint, random
        from contexttimer import Timer
        from uuid import uuid4

        fs = fsopendir('mem://')

        # fs = fsopendir('/tmp/pmpf')

        N = 50000

        # Basic read/ write tests.

        row = lambda: [None, 1, random(), str(uuid4()),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10)]
        headers = list('abcdefghi')[:len(row())]

        rows = [row() for i in range(N)]

        with Timer() as t:
            df = MPRowsFile(fs, 'foobar')
            w = df.writer

            w.headers = headers

            w.meta['source']['url'] = 'blah blah'

            for i in range(N):
                w.insert_row(rows[i])

            w.close()

        print "MSGPack write ", float(N) / t.elapsed, w.n_rows

        with Timer() as t:
            count = 0
            i = 0
            s = 0

            r = df.reader

            for i, row in enumerate(r):
                count += 1

            r.close()

        print "MSGPack read  ", float(N) / t.elapsed, i, count, s

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.rows:
                count += 1

            r.close()

        print "MSGPack rows  ", float(N) / t.elapsed

        with Timer() as t:
            count = 0

            r = df.reader

            for row in r.raw:
                count += 1

            r.close()

        print "MSGPack raw   ", float(N) / t.elapsed

    def x_test_mpr_meta(self):

        # Saving code for later.
        r = None
        df = None

        self.assertEqual('blah blah', r.meta['source']['url'])

        w = df.writer

        w.meta['source']['url'] = 'bingo'

        w.close()

        r = df.reader

        self.assertEqual('bingo', r.meta['source']['url'])

    def generate_rows(self, N):

        import time
        import datetime
        from random import randint, random
        from uuid import uuid4

        row = lambda x: [x, x*2, random(), str(uuid4()),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10),
                       datetime.date(randint(2000, 2015), randint(1, 12), 10)]

        headers = list('abcdefghi')[:len(row(0))]

        rows = [row(i) for i in range(1,N+1)]

        return rows, headers

    def test_stats(self):
        """Check that the soruces can be loaded and analyzed without exceptions and that the
        guesses for headers and start are as expected"""

        from ambry_sources.mpf import MPRowsFile
        from ambry_sources import get_source

        from contexttimer import Timer

        cache_fs = fsopendir('mem://')
        # cache_fs = fsopendir('/tmp/ritest/')

        sources = self.load_sources('sources-non-std-headers.csv')

        for source_name, spec in sources.items():

            s = get_source(spec, cache_fs)

            #if source_name != 'active': continue

            print spec.name, spec.url

            with Timer() as t:
                f = MPRowsFile(cache_fs, source_name).load_rows(s, run_stats = True)

            with f.reader as r:
                print "Loaded ", r.n_rows, float(r.n_rows)/ t.elapsed, 'rows/sec'

            with f.reader as r:
                stats = r.meta['stats']

                #print [ sd['mean'] for col_name, sd in r.meta['stats'].items() ]



    def test_datafile(self):
        """
        Test Loading and interating over data files, exercising the three header cases, and the use
        of data start and end lines.

        :return:
        """
        from ambry_sources.mpf import MPRowsFile
        from ambry_sources.sources import ColumnSpec
        from itertools import islice

        N = 500

        rows, headers = self.generate_rows(N)

        def first_row_header(data_start_row=None, data_end_row = None):

            # Normal Headers
            f = MPRowsFile('mem://frh')
            w = f.writer

            w.insert_headers(headers)

            for row in rows:
                w.insert_row(row)

            if data_start_row is not None:
                w.data_start_row = data_start_row

            if data_end_row is not None:
                w.data_end_row = data_end_row

            w.close()

            self.assertEquals([u'a', u'b', u'c', u'd', u'e', u'f'], w.parent.reader.headers)

            w.parent.reader.close()

            return f

        def no_header(data_start_row=None, data_end_row = None):

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

            self.assertEquals(['col0', 'col1', 'col2', 'col3', 'col4', 'col5'], w.parent.reader.headers)

            w.parent.reader.close()

            return f

        def schema_header(data_start_row=None, data_end_row = None):
            # Set the schema
            f = MPRowsFile('mem://sh')
            w = f.writer

            w.meta['schema'] = [dict(name = 'x'+str(e)) for e in range(len(headers))]

            for row in rows:
                w.insert_row(row)

            if data_start_row is not None:
                w.data_start_row = data_start_row

            if data_end_row is not None:
                w.data_end_row = data_end_row

            w.close()

            self.assertEquals([u'x0', u'x1', u'x2', u'x3', u'x4', u'x5'], w.parent.reader.headers)

            w.parent.reader.close()

            return f

        # Try a few header start / data start values.

        for ff in ( first_row_header, schema_header, no_header):
            print '===', ff
            f = ff()

            with f.reader as r:
                self.assertEqual(N, len(list(r.rows)))

            with f.reader as r:
                # Check that the first row value starts at one and goes up from there.
                map(lambda f: self.assertEqual(f[0], f[1][0]), enumerate(islice(r.rows, 5),1))

        for ff in (first_row_header, schema_header, no_header):
            print '===', ff
            data_start_row = 5
            data_end_row = 15
            f = ff(data_start_row, data_end_row)

            with f.reader as r:
                l = list(r.rows)
                self.assertEqual(11, len(l))

            with f.reader as r:
                #Check that the first row value starts at one and goes up from there.
                # the - r.info['header_row'] bit accounts for the fact that sometimes the header is the first row,
                # sometimes not.
                map(lambda f: self.assertEqual(f[0], f[1][0]), enumerate(list(r.rows)[:5],
                                                                         data_start_row - r.info['header_row'] ))

            with f.reader as r:
                self.assertEquals(data_end_row - r.info['header_row'], list(r.rows)[-1][0])


if __name__ == '__main__':
    unittest.main()
