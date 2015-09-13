# -*- coding: utf-8 -*-

import os

from fs.opener import fsopendir

from attrdict import AttrDict

from ambry_sources.mpf import MPRowsFile
from ambry_sources.sources import SourceSpec, CsvSource, DelayedOpen

from tests import TestBase

TEST_FILES_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), 'files'))


class BaseMEDTest(TestBase):

    def _get_fake_datetime_partition(self, vid):
        """ Creates fake partition with int, date, datetime fields. """
        table = AttrDict(
            columns=[
                {'name': 'rowid', 'type': 'int', 'primary_key': True},
                {'name': 'col1', 'type': 'date'},
                {'name': 'col2', 'type': 'datetime'}])
        datafile = AttrDict(
            syspath=os.path.join(TEST_FILES_DIR, 'rowid_int_col1_date_col2_datetime_100_rows_gzipped.mpr'))
        partition = AttrDict(vid=vid, table=table, datafile=datafile)
        return partition

    def _get_fake_partition(self, vid):
        """ Creates fake partition from test msgpack file with int, int, str fields. """
        table = AttrDict(
            columns=[
                {'name': 'rowid', 'type': 'int', 'primary_key': True},
                {'name': 'col1', 'type': 'int'},
                {'name': 'col2', 'type': 'str', 'max_length': '8'}])
        datafile = AttrDict(
            syspath=os.path.join(TEST_FILES_DIR, 'rowid_int_col1_int_col2_str_100_rows_gzipped.mpr'))
        partition = AttrDict(vid=vid, table=table, datafile=datafile)
        return partition

    def _get_partition_mpr(self, file_name, partition_vid=None):
        """ Converts csv with given file name to the MPR and returns it.

        Args:
            file_name (str):

        Returns:
            MPRowsFile:

        """
        SPEC_MAP = {
            'col1_int_col2_str_100_rows.csv': SourceSpec(
                'http://example.com', expect_start='2', name='test', expect_headers='1'),
            'col1_date_col2_datetime_100_rows.csv': SourceSpec(
                'http://example.com', expect_start='2', name='test', expect_headers='1')}
        memory_fs = fsopendir('mem://')
        files_fs = fsopendir(TEST_FILES_DIR)
        fstor = DelayedOpen(files_fs, file_name, 'rb')
        spec = SPEC_MAP[file_name]
        source = CsvSource(spec, fstor)
        f = MPRowsFile(memory_fs, '/mpr/' + file_name + '.msg').load_rows(source)
        # FIXME: remove partition_vid
        f.partition_vid = partition_vid
        return f
