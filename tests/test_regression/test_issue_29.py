# -*- coding: utf-8 -*-

import apsw

from fs.opener import fsopendir

from ambry_sources.med.sqlite import add_partition
from ambry_sources.mpf import MPRowsFile
from ambry_sources.sources import GeneratorSource, SourceSpec

from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/29 issue.


class Test(TestBase):

    def test_creates_virtual_table_for_source_with_header_containing_sql_reserved_words(self):
        # build rows reader
        cache_fs = fsopendir(self.setup_temp_dir())

        spec = SourceSpec('foobar')

        def gen():

            # yield header
            yield ['create', 'index', 'where', 'select', 'distinct']

            # yield rows
            for i in range(10):
                yield [i, i + 1, i + 2, i + 3, i + 4]

        s = GeneratorSource(spec, gen())
        mprows = MPRowsFile(cache_fs, spec.name).load_rows(s)

        connection = apsw.Connection(':memory:')
        table = 'table1'
        add_partition(connection, mprows, table)

        # check all columns and some rows.
        cursor = connection.cursor()
        query = 'SELECT count(*) FROM {};'.format(table)
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, [(10,)])

        with mprows.reader as r:
            expected_first_row = next(iter(r)).row

        # query by columns.
        query = 'SELECT "create", "index", "where", "select", "distinct" FROM {} LIMIT 1;'.format(table)
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], expected_first_row)
