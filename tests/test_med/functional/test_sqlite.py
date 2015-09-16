# -*- coding: utf-8 -*-

import apsw

from ambry_sources import get_source
from ambry_sources.med.sqlite import add_partition, _table_name
from ambry_sources.mpf import MPRowsFile

from tests import TestBase
from fs.opener import fsopendir


class Test(TestBase):

    def test_creates_virtual_table_for_simple_fixed_mpr(self):
        # build rows reader
        cache_fs = fsopendir(self.setup_temp_dir())
        sources = self.load_sources()
        spec = sources['simple_fixed']
        s = get_source(spec, cache_fs)
        partition = MPRowsFile(cache_fs, spec.name).load_rows(s)

        # first make sure file not changed.
        expected_names = ['id', 'uuid', 'int', 'float']
        expected_types = ['str', 'str', 'str', 'float']
        self.assertEqual(sorted([x['name'] for x in partition.schema]), sorted(expected_names))
        self.assertEqual(sorted([x['type'] for x in partition.schema]), sorted(expected_types))

        connection = apsw.Connection(':memory:')
        add_partition(connection, partition)

        # check all columns and some rows.
        cursor = connection.cursor()
        query = 'SELECT count(*) FROM {};'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(result, [(10000,)])

        # query by columns.
        query = 'SELECT id, uuid, int, float FROM {} LIMIT 1;'.format(_table_name(partition))
        result = cursor.execute(query).fetchall()
        self.assertEqual(len(result), 1)
        expected_first_row = ('1eb385', 'c36-9298-4427-8925-fe09294dbd 30', '99.', '734691532')
        self.assertEqual(result[0], expected_first_row)
