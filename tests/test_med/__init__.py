# -*- coding: utf-8 -*-

import os

from attrdict import AttrDict

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
