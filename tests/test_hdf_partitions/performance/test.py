# -*- coding: utf-8 -*-

from fs.opener import fsopendir

import pytest

from ambry_sources.hdf_partitions.core import HDFPartition

from tests import TestBase


class Test(TestBase):

    @pytest.mark.slow
    def test_datafile_read_write(self):
        import datetime
        from random import randint, random
        from contexttimer import Timer
        from uuid import uuid4

        fs = fsopendir('temp://')

        N = 50000

        # Basic read/ write tests.

        def rand_date_a():
            return datetime.date(randint(2000, 2015), randint(1, 12), 10)

        epoch = datetime.date(1970, 1, 1)

        def rand_date_b():
            return (datetime.date(randint(2000, 2015), randint(1, 12), 10) - epoch).total_seconds()

        row = lambda: (0, 1, random(), str(uuid4()), rand_date_b(), rand_date_b())

        headers = list('abcdefghi')[:len(row())]

        rows = [row() for i in range(N)]

        def write_large_blocks():

            df = HDFPartition(fs, path='foobar')

            if df.exists:
                df.remove()
            with Timer() as t, df.writer as w:
                w.headers = headers
                type_index = w.meta['schema'][0].index('type')
                pos_index = w.meta['schema'][0].index('pos')
                columns = w.meta['schema'][1:]
                for column in columns:
                    column[type_index] = type(rows[0][column[pos_index] - 1]).__name__
                w.insert_rows(rows)

            print('HDF write ', float(N) / t.elapsed, w.n_rows)

        def write_small_blocks():
            df = HDFPartition(fs, path='foobar')

            if df.exists:
                df.remove()

            with Timer() as t, df.writer as w:
                w.headers = headers
                type_index = w.meta['schema'][0].index('type')
                pos_index = w.meta['schema'][0].index('pos')
                columns = w.meta['schema'][1:]
                for column in columns:
                    column[type_index] = type(rows[0][column[pos_index] - 1]).__name__
                for i in range(N):
                    w.insert_row(rows[i])
            print('HDF write ', float(N) / t.elapsed, w.n_rows)

        write_large_blocks()

        write_small_blocks()

        # timing reader.
        df = HDFPartition(fs, 'foobar')

        with Timer() as t:
            count = 0
            i = 0
            s = 0
            r = df.reader
            for i, row in enumerate(r):
                count += 1
            r.close()

        print('HDFPartition read  ', float(N) / t.elapsed, i, count, s)

        with Timer() as t:
            count = 0
            r = df.reader
            for row in r.rows:
                count += 1
            r.close()

        print('HDFPartition rows  ', float(N) / t.elapsed)

        with Timer() as t:
            count = 0
            r = df.reader
            for row in r.raw:
                count += 1
            r.close()
        print('HDFPartition raw   ', float(N) / t.elapsed)
