# -*- coding: utf-8 -*-
"""
Writing data to a HDF partition.
"""

from copy import deepcopy
from functools import reduce
import json
import logging
import math
import time
import os
import re

from tables import open_file, StringCol, Int64Col, Float64Col, BoolCol, Int32Col
from tables.exceptions import NoSuchNodeError
import numpy as np

import six
from six import string_types, iteritems, text_type, binary_type

from ambry_sources.sources import RowProxy
from ambry_sources.stats import Stats
from ambry_sources.mpf import MPRowsFile

logger = logging.getLogger(__name__)

# pytables can't store None for ints, so use minimal int value to store None.
MIN_INT32 = np.iinfo(np.int32).min
MIN_INT64 = np.iinfo(np.int64).min


class HDFError(Exception):
    pass


class HDFPartition(object):
    """ Stores partition data in the HDF (*.h5) file. """

    EXTENSION = '.h5'
    VERSION = 1

    def __init__(self, url_or_fs, path=None):
        """

        Args:
            url_or_fs (str or filesystem):
            path (str):
        """
        from fs.opener import opener

        if path:
            self._fs, self._path = url_or_fs, path
        else:
            self._fs, self._path = opener.parse(url_or_fs)

        if not self._fs.hassyspath(''):
            # Pytables requirement.
            raise HDFError('HDFPartition requires filesystem having sys path.')

        self._writer = None
        self._reader = None

        self._process = None  # Process name for report_progress
        self._start_time = 0

        if not self._path.endswith(self.EXTENSION):
            self._path = self._path + self.EXTENSION

    @property
    def path(self):
        return self._path

    @property
    def syspath(self):
        if self.exists:
            return self._fs.getsyspath(self.path)
        else:
            return None

    @property
    def info(self):
        return self._info(self.reader)

    @property
    def exists(self):
        return self._fs.exists(self.path)

    def remove(self):
        if self.exists:
            self._fs.remove(self._path)

    @property
    def meta(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.meta

    @property
    def stats(self):
        return (self.meta or {}).get('stats')

    @property
    def n_rows(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.n_rows

    @property
    def headers(self):

        if not self.exists:
            return None

        with self.reader as r:
            return r.headers

    def run_stats(self):
        """Run the stats process and store the results back in the metadata"""

        try:
            self._process = 'run_stats'
            self._start_time = time.time()

            with self.reader as r:
                stats = Stats([(c.name, c.type) for c in r.columns]).run(r, sample_from=r.n_rows)

            with self.writer as w:
                w.set_stats(stats)

        finally:
            self._process = 'none'

        return stats

    def load_rows(self, source, run_stats=True):
        """ Loads rows from given source.

        Args:
            source (SourceFile):
            run_stats (boolean, optional): if True then collect stat and save it to meta.

        Returns:
            HDFPartition:

        """
        if self.n_rows:
            raise HDFError("Can't load_rows; rows already loaded. n_rows = {}".format(self.n_rows))

        # None means to determine True or False from the existence of a row spec
        try:

            self._process = 'load_rows'
            self._start_time = time.time()

            with self.writer as w:
                w.load_rows(source)

            if run_stats:
                self.run_stats()
        finally:
            self._process = None

        return self

    @property
    def reader(self):
        if not self._reader:
            self._reader = HDFReader(self, self.syspath)
        return self._reader

    def __iter__(self):
        """ Iterate over a reader. """

        # There is probably a more efficient way in python 2 to do this than to have another yield loop,
        # but just returning the reader iterator doesn't work
        with self.reader as r:
            for row in r:
                yield row

    def select(self, predicate=None, headers=None):
        """Iterate the results from the reader's select() method"""

        with self.reader as r:
            for row in r.select(predicate, headers):
                yield row

    @property
    def writer(self):
        if not self._writer:
            self._process = 'write'

            if not self._fs.exists(os.path.dirname(self.path)):
                self._fs.makedir(os.path.dirname(self.path), recursive=True)

            # we can't use self.syspath here because it may be empty if file does not existf
            self._writer = HDFWriter(self, self._fs.getsyspath(self.path))

        return self._writer

    def report_progress(self):
        """
        This function can be called from a higher level to report progress. It is usually called from an alarm
        signal handler which is installed just before starting a load_rows operation:

        >>> import signal
        >>> f = HDFPartition('tmp://foobar')
        >>> def handler(signum, frame):
        >>>     print "Loading: %s, %s rows" % f.report_progress()
        >>> f.load_rows( [i,i,i] for i in range(1000))

        :return: Tuple: (process description, #records, #total records, #rate)
        """

        rec = total = rate = 0

        if self._process in ('load_rows', 'write') and self._writer:
            rec = self._writer.n_rows
            rate = round(float(rec) / float(time.time() - self._start_time), 2)

        elif self._reader:
            rec = self._reader.pos
            total = self._reader.data_end_row
            rate = round(float(rec) / float(time.time() - self._start_time), 2)

        return (self._process, rec, total, rate)

    @classmethod
    def _columns(cls, o, n_cols=0):
        """ Wraps columns from meta['schema'] with RowProxy and generates them.

        Args:
            o (any having .meta dict attr):

        Generates:
            RowProxy: column wrapped with RowProxy

        """
        s = o.meta['schema']

        assert len(s) >= 1  # Should always have header row.
        assert o.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE, (o.meta['schema'][0], MPRowsFile.SCHEMA_TEMPLATE)

        # n_cols here is for columns in the data table, which are rows in the headers table
        n_cols = max(n_cols, o.n_cols, len(s) - 1)

        for i in range(1, n_cols + 1):
            # Normally, we'd only create one of these, and set the row on the singleton for
            # each row. But in this case, the caller may turn the output of the method into a list,
            # in which case all of the rows would have the values of the last one.
            rp = RowProxy(s[0])
            try:
                row = s[i]
            except IndexError:
                # Extend the row, but make sure the pos value is set property.
                ext_row = [i, 'col{}'.format(i)] + [None] * (len(s[0]) - 2)
                s.append(ext_row)
                row = s[i]

            yield rp.set_row(row)

        assert o.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

    @classmethod
    def _info(cls, o):
        return dict(
            version=o.version,
            data_start_pos=0,
            meta_start_pos=0,
            rows=o.n_rows,
            cols=o.n_cols,
            header_rows=o.meta['row_spec']['header_rows'],
            data_start_row=0,
            data_end_row=None,
            comment_rows=o.meta['row_spec']['comment_rows'],
            headers=o.headers
        )


class HDFWriter(object):

    def __init__(self, parent, filename):

        if not isinstance(filename, string_types):
            raise ValueError(
                'Pytables requires filename parameter as string. Got {} instead.'
                .format(filename.__class__))

        self.parent = parent
        self.version = HDFPartition.VERSION

        self.n_rows = 0
        self.n_cols = 0

        self.cache = []

        if os.path.exists(filename):
            self._h5_file = open_file(filename, mode='a')
            self.meta = HDFReader._read_meta(self._h5_file)
            self.version, self.n_rows, self.n_cols = _get_file_header(
                self._h5_file.root.partition.file_header)
        else:
            # No, doesn't exist
            self._h5_file = open_file(filename, mode='w')
            self.meta = deepcopy(MPRowsFile.META_TEMPLATE)

        self.header_mangler = lambda name: re.sub('_+', '_', re.sub('[^\w_]', '_', name).lower()).rstrip('_')

        if self.n_rows == 0:
            self.meta['about']['create_time'] = time.time()

    @property
    def info(self):
        return HDFPartition._info(self)

    def set_col_val(name_or_pos, **kwargs):
        pass

    @property
    def headers(self):
        """ Return the headers rows. """
        return [e.name for e in HDFPartition._columns(self)]

    @headers.setter
    def headers(self, headers):
        """ Set column names. """

        if not headers:
            return

        assert isinstance(headers,  (tuple, list)), headers

        for i, row in enumerate(HDFPartition._columns(self, len(headers))):
            assert isinstance(headers[i], string_types)
            row.name = headers[i]

        assert self.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE

    @property
    def columns(self):
        """ Returns the columns specifications. """
        return HDFPartition._columns(self)

    @columns.setter
    def columns(self, headers):

        for i, row in enumerate(HDFPartition._columns(self, len(headers))):

            h = headers[i]

            if isinstance(h, dict):
                raise NotImplementedError()
            else:
                row.name = h

    def column(self, name_or_pos):

        for h in self.columns:

            if name_or_pos == h.pos or name_or_pos == h.name:
                return h

        raise KeyError("Didn't find '{}' as either a name nor a position ".format(name_or_pos))

    def insert_row(self, row):

        self.n_rows += 1

        self.cache.append(row)

        if len(self.cache) >= 10000:
            self._write_rows()

    def insert_rows(self, rows):
        """ Inserts a list of rows. Does not insert iterators.

        Args:
            rows (list of list):

        """
        self.n_rows += len(rows)
        self._write_rows(rows)

    def load_rows(self, source):
        """ Loads rows from an iterator.

        Args:
            source (iterator):
            columns (list of intuit.Column): schema (columns description) of the source.

        """
        spec = getattr(source, 'spec', None)
        for i, row in enumerate(iter(source)):
            if spec and i < (spec.start_line or 1):
                # skip comments and headers. If start line is empty, assuming first row is header.
                continue

            if spec and spec.end_line and i > spec.end_line:
                # skip footer
                break
            self.insert_row(row)

        # If the source has a headers property, and it's defined, then
        # use it for the headers. This often has to be called after iteration, because
        # the source may have the header as the first row
        try:
            if source.headers:
                self.headers = source.headers
        except AttributeError:
            pass

        self._write_rows()

    def close(self):

        if self._h5_file:
            self._write_rows()
            self._h5_file.close()
            self._h5_file = None

            if self.parent:
                self.parent._writer = None

    def write_file_header(self):
        """ Write the version, number of rows and number of cols to the h5 file. """

        if 'file_header' in self._h5_file.root.partition:
            self._h5_file.remove_node('/partition', 'file_header')

        descriptor = {
            'version': Int32Col(),
            'n_rows': Int32Col(),
            'n_cols': Int32Col()
        }

        table = self._h5_file.create_table(
            '/partition', 'file_header',
            descriptor, 'Header of the file.')

        table.row['version'] = HDFPartition.VERSION
        table.row['n_rows'] = self.n_rows
        table.row['n_cols'] = self.n_cols
        table.row.append()
        table.flush()

    def set_types(self, ti):
        """ Set Types from a type intuiter object. """

        results = {int(r['position']): r for r in ti._dump()}

        for i in range(len(results)):

            for k, v in iteritems(results[i]):
                k = {'count': 'type_count'}.get(k, k)
                self.column(i + 1)[k] = v

            if not self.column(i + 1).type:
                self.column(i + 1).type = results[i]['resolved_type']

    def set_stats(self, stats):
        """ Copy stats into the schema.

        Args:
            stats (Stats):

        """

        for name, stat_set in iteritems(stats.dict):
            row = self.column(name)

            for k, v in iteritems(stat_set.dict):
                k = {'count': 'stat_count'}.get(k, k)
                row[k] = v

    def set_source_spec(self, spec):
        """Set the metadata coresponding to the SourceSpec, excluding the row spec parts. """

        ms = self.meta['source']

        ms['url'] = spec.url
        ms['fetch_time'] = spec.download_time
        ms['file_type'] = spec.filetype
        ms['url_type'] = spec.urltype
        ms['encoding'] = spec.encoding

        me = self.meta['excel']
        me['worksheet'] = spec.segment

        if spec.columns:

            for i, sc in enumerate(spec.columns, 1):
                c = self.column(i)

                if c.name:
                    assert sc.name == c.name

                c.start = sc.start
                c.width = sc.width

    def set_row_spec(self, row_spec, headers):
        """ Saves row_spec to meta and populates headers.

        Args:
            row_spec (dict): dict with rows specifications
                Example: {
                    'header_rows': [1,2],
                    'comment_rows': [0],
                    'start_row': 3,
                    'end_row': None,
                    'data_pattern': ''
                }

        """

        self.data_start_row = row_spec['start_row']
        self.data_end_row = row_spec['end_row']
        self.meta['row_spec'] = row_spec
        self.headers = [self.header_mangler(h) for h in headers]
        self._write_meta()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False

    def _validate_groups(self):
        """ Checks and creates needded groups in the h5 file. """
        if 'partition' not in self._h5_file.root:
            self._h5_file.create_group('/', 'partition', 'Partition.')
        if 'meta' not in self._h5_file.root.partition:
            self._h5_file.create_group('/partition', 'meta', 'Meta information of the partition.')

    def _write_rows(self, rows=None):
        self._write_meta()
        self.write_file_header()
        rows, clear_cache = (self.cache, True) if not rows else (rows, False)

        if not rows:
            return

        # convert columns to descriptor
        rows_descriptor = _get_rows_descriptor(self.columns)

        if 'rows' not in self._h5_file.root.partition:
            self._h5_file.create_table(
                '/partition', 'rows', rows_descriptor, 'Rows (data) of the partition.')

        rows_table = self._h5_file.root.partition.rows
        partition_row = rows_table.row

        # h5 colnames order has to match to columns order to provide proper iteration over rows.
        assert self.headers == rows_table.colnames
        description = [
            (col_name, getattr(rows_table.description, col_name)) for col_name in rows_table.colnames]
        for row in rows:
            for col_name, col_desc in description:
                value = _serialize(col_desc.__class__, row[col_desc._v_pos])
                if isinstance(value, text_type):
                    value = value.encode('utf-8')
                partition_row[col_name] = value
            partition_row.append()
        rows_table.flush()

        # Hope that the max # of cols is found in the first 100 rows
        # FIXME! This won't work if rows is an interator.
        self.n_cols = reduce(max, (len(e) for e in rows[:100]), self.n_cols)

        if clear_cache:
            self.cache = []

    def _write_meta(self):
        """ Writes meta to the h5 file. """
        assert self.meta['schema'][0] == MPRowsFile.SCHEMA_TEMPLATE
        self._validate_groups()
        self._save_about()
        self._save_comments()
        self._save_excel()
        self._save_geo()
        self._save_row_spec()
        self._save_schema()
        self._save_source()

    def _save_meta_child(self, child, descriptor):
        """ Saves given child of the meta to the table with same name to the h5 file.

        Args:
            child (str): name of the child.
            descriptor (dict): descriptor of the table.

        """
        # always re-create table on save. It works better than rows removing.
        if child in self._h5_file.root.partition.meta:
            self._h5_file.remove_node('/partition/meta', child)

        table = self._h5_file.create_table(
            '/partition/meta', child,
            descriptor, 'meta.{}'.format(child))
        row = table.row
        for k, v in self.meta[child].items():
            if k in ('header_rows', 'comment_rows'):
                v = json.dumps(v or '')
            row[k] = _serialize(descriptor[k].__class__, v)
        row.append()
        table.flush()

    def _save_about(self):
        descriptor = {
            'load_time': Float64Col(),
            'create_time': Float64Col()
        }
        self._save_meta_child('about', descriptor)

    def _save_schema(self):
        """ Saves meta.schema table of the h5 file.
        """
        descriptor = {
            'pos': Int32Col(),
            'name': StringCol(itemsize=255),
            'type': StringCol(itemsize=255),
            'description': StringCol(itemsize=1024),
            'start': Int32Col(),
            'width': Int32Col(),
            'position': Int32Col(),
            'header': StringCol(itemsize=255),
            'length': Int32Col(),
            'has_codes': BoolCol(),
            'type_count': Int32Col(),
            'ints': Int32Col(),
            'floats': Int32Col(),
            'strs': Int32Col(),
            'unicode': Int32Col(),
            'nones': Int32Col(),
            'datetimes': Int32Col(),
            'dates': Int32Col(),
            'times': Int32Col(),
            'strvals': StringCol(itemsize=255),
            'flags': StringCol(itemsize=255),
            'lom': StringCol(itemsize=1),
            'resolved_type': StringCol(itemsize=40),
            'stat_count': Int32Col(),
            'nuniques': Int32Col(),
            'mean': Float64Col(),
            'std': Float64Col(),
            'min': Float64Col(),
            'p25': Float64Col(),
            'p50': Float64Col(),
            'p75': Float64Col(),
            'max': Float64Col(),
            'skewness': Float64Col(),
            'kurtosis': Float64Col(),
            'hist': StringCol(itemsize=255),
            'text_hist': StringCol(itemsize=255),
            'uvalues': StringCol(itemsize=5000)
        }
        # always re-create table on save. It works better than rows removing.
        if 'schema' in self._h5_file.root.partition.meta:
            self._h5_file.remove_node('/partition/meta', 'schema')

        self._h5_file.create_table(
            '/partition/meta', 'schema',
            descriptor, 'meta.schema',
            createparents=True)

        schema = self.meta['schema'][0]
        table = self._h5_file.root.partition.meta.schema
        row = table.row

        for col_descr in self.meta['schema'][1:]:
            for i, col_name in enumerate(schema):
                if col_name in ('hist', 'uvalues'):
                    value = json.dumps(col_descr[i] or '')
                else:
                    value = _serialize(descriptor[col_name].__class__, col_descr[i])
                    if isinstance(value, text_type):
                        value = value.encode('utf-8')
                row[col_name] = value
            row.append()
        table.flush()

    def _save_excel(self):
        descriptor = {
            'worksheet': StringCol(itemsize=255),
            'datemode': Int32Col()
        }
        self._save_meta_child('excel', descriptor)

    def _save_comments(self):
        descriptor = {
            'header': StringCol(itemsize=255),
            'footer': StringCol(itemsize=255)
        }
        self._save_meta_child('comments', descriptor)

    def _save_source(self):
        descriptor = {
            'fetch_time': Float64Col(),
            'encoding': StringCol(itemsize=255),
            'url': StringCol(itemsize=1024),
            'file_type': StringCol(itemsize=50),
            'inner_file': StringCol(itemsize=255),
            'url_type': StringCol(itemsize=255),
        }
        self._save_meta_child('source', descriptor)

    def _save_row_spec(self):
        descriptor = {
            'end_row': Int32Col(),
            'header_rows': StringCol(itemsize=255),  # comma separated ints or empty string.
            'start_row': Int32Col(),
            'comment_rows': StringCol(itemsize=255),  # comma separated ints or empty string.
            'data_pattern': StringCol(itemsize=255)
        }
        self._save_meta_child('row_spec', descriptor)

    def _save_geo(self):
        descriptor = {
            'srs': Int32Col(),
            'bb': Int32Col(),
        }
        self._save_meta_child('geo', descriptor)


class HDFReader(object):
    """ Read an h5 file. """

    def __init__(self, parent, filename):
        """ Reads the filename and prepares for iterating over rows.

        Args:
            parent (HDFPartition):
            filename (str):

        """

        if not isinstance(filename, string_types):
            # This is the pytables constraint.
            raise ValueError(
                'HDFReader requires string with filename. Got {} instead.'
                .format(filename.__class__))

        self.parent = parent
        self._h5_file = open_file(filename, mode='r')
        self._headers = None

        self.pos = 0  # Row position for next read.

        self.n_rows = 0
        self.n_cols = 0
        self.version, self.n_rows, self.n_cols = _get_file_header(self._h5_file.root.partition.file_header)

        self._in_iteration = False
        self._meta = None

    @property
    def info(self):
        return HDFPartition._info(self)

    @property
    def meta(self):
        if self._meta is None:
            self._meta = self._read_meta(self._h5_file)
        return self._meta

    @property
    def columns(self):
        """ Returns columns specifications in the ambry_source format. """
        return HDFPartition._columns(self)

    @property
    def headers(self):
        """ Returns header (column names).

        Returns:
            list of str:

        """
        return [e.name for e in HDFPartition._columns(self)]

    @property
    def raw(self):
        """ A raw iterator, which ignores the data start and stop rows and returns all rows, as rows. """
        try:
            if 'rows' not in self._h5_file.root.partition:
                # table with rows was not created.
                raise StopIteration
            self._in_iteration = True
            table = self._h5_file.root.partition.rows
            for row in table.iterrows():
                yield [row[c] for c in table.colnames]
                self.pos += 1
        finally:
            self._in_iteration = False
            self.close()

    @property
    def rows(self):
        """ Iterator for reading rows. """
        # For HDF it's exactly the same as raw iterator.
        return self.raw

    def __iter__(self):
        """ Iterator for reading rows as RowProxy objects

        WARNING: This routine generates RowProxy objects. RowProxy objects
            are reused, so if you construct a list directly from the output from this method,
            the list will have multiple copies of a single RowProxy, which will
            have as an inner row the last result row. If you will be directly constructing
            a list, use a getter that extracts the inner row, or which converted the RowProxy
            to a dict.

        """
        rp = RowProxy(self.headers)
        try:
            if 'rows' not in self._h5_file.root.partition:
                # rows table was not created.
                raise StopIteration
            self._in_iteration = True
            table = self._h5_file.root.partition.rows
            for row in table.iterrows():
                r = [_deserialize(row[c]) for c in table.colnames]
                yield rp.set_row(r)
                self.pos += 1
        finally:
            self._in_iteration = False

    def select(self, predicate=None, headers=None):
        """ Select rows from the reader using a predicate and itemgetter to return a subset of elements.

        Args:
            predicate (callable, optional): if defined, a callable that is called for each rowm and
                if it returns true, the row is included in the output.
            headers (list, optional): if defined, a list or tuple of header names to return from each row

        Returns:
            iterable: iterable of results

        WARNING: This routine works from the reader iterator, which returns RowProxy objects. RowProxy
            objects are reused, so if you construct a list directly from the output from
            this method, the list will have multiple copies of a single RowProxy,
            which will have as an inner row the last result row. If you will
            be directly constructing a list, use a getter that extracts the inner row, or which
            converted the RowProxy to a dict:

            list(s.datafile.select(lambda r: r.stusab == 'CA', lambda r: r.dict))

        """

        if headers:
            from operator import itemgetter
            ig = itemgetter(*headers)
            rp = RowProxy(headers)
            getter = lambda r: rp.set_row(ig(r.dict))
        else:
            getter = None

        if getter is not None and predicate is not None:
            return six.moves.map(getter, filter(predicate, iter(self)))

        elif getter is not None and predicate is None:
            return six.moves.map(getter, iter(self))

        elif getter is None and predicate is not None:
            return six.moves.filter(predicate, self)
        else:
            return iter(self)

    def close(self):
        if self._h5_file:
            self.meta  # In case caller wants to read meta after close.
            self._h5_file.close()
            self._h5_file = None
            if self.parent:
                self.parent._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_val:
            return False

    @classmethod
    def _read_meta(self, h5_file):
        meta = deepcopy(MPRowsFile.META_TEMPLATE)
        for child, group in meta.items():
            if child == 'schema':
                # This is the special case because meta.schema construct from many rows.
                new_schema = [MPRowsFile.SCHEMA_TEMPLATE]
                for col_descr in self._read_meta_child(h5_file, 'schema'):
                    col = []
                    for e in MPRowsFile.SCHEMA_TEMPLATE:
                        col.append(_deserialize(col_descr.get(e)))
                    new_schema.append(col)
                meta['schema'] = new_schema
            else:
                # This is the common case when child of the meta constructs from exactly one row.
                try:
                    saved_data = self._read_meta_child(h5_file, child)
                    if saved_data:
                        saved_data = saved_data[0]
                    else:
                        saved_data = {}
                except NoSuchNodeError:
                    logger.warning('meta.{} table does not exist. Using default values.'.format(child))
                    saved_data = {}
                for k, default_value in group.items():
                    meta[child][k] = saved_data.get(k, default_value)
        return meta

    @classmethod
    def _read_meta_child(self, h5_file, child):
        """ Reads all rows from `child` table of h5 file and returns it.

        Args:
            child (str): name of the table from h5 file.

        Returns:
            dict:
        """
        table = getattr(h5_file.root.partition.meta, child)
        ret = []
        for row in table.iterrows():
            elem = {}
            for c in table.colnames:
                v = _deserialize(row[c])
                if c in ('header_rows', 'comment_rows', 'hist', 'uvalues'):
                    v = json.loads(v)
                elem[c] = v
            ret.append(elem)
        return ret


def _get_rows_descriptor(columns):
    """ Converts columns specifications from ambry_sources format to pytables descriptor.

    Args:
        columns (list of dict)

    Returns:
        dict: valid pytables descriptor.
    """
    TYPE_MAP = {
        'int': lambda pos: Int32Col(pos=pos),
        'long': lambda pos: Int64Col(pos=pos),
        'str': lambda pos: StringCol(itemsize=255, pos=pos),
        'bytes': lambda pos: StringCol(itemsize=255, pos=pos),
        'float': lambda pos: Float64Col(pos=pos),
        'unknown': lambda pos: StringCol(itemsize=255, pos=pos),
    }
    descriptor = {}

    for column in columns:
        pytables_type = TYPE_MAP.get(column['type'])
        if not pytables_type:
            raise Exception(
                'Failed to convert `{}` ambry_sources type to pytables type.'.format(column['type']))
        descriptor[column['name']] = pytables_type(column['pos'])
    return descriptor


def _serialize(col_type, value):
    """ Converts value to format ready to save to h5 file. """
    if col_type == Float64Col:
        try:
            float(value)
        except (TypeError, ValueError):
            # it is not a valid float.
            value = None

    if col_type in (Int32Col, Int64Col):
        try:
            int(value)
        except (TypeError, ValueError):
            # it is not a valid int.
            value = None

    TYPE_MAP = {
        Int64Col: MIN_INT64,
        Int32Col: MIN_INT32,
        Float64Col: float('nan'),
        StringCol: '',
    }
    force = False

    if value is None:
        force = True

    elif isinstance(value, string_types) and value == 'NA':
        force = True

    if force and col_type in TYPE_MAP:
        return TYPE_MAP[col_type]

    return value


def _deserialize(value):
    """ Converts None replacements stored in the pytables to None. """
    if isinstance(value, six.integer_types) and value in (MIN_INT32, MIN_INT64):
        return None
    elif isinstance(value, float) and math.isnan(value):
        return None
    elif isinstance(value, binary_type):
        return value.decode('utf-8')
    return value


def _get_file_header(table):
    """ Returns tuple with file headers - (version, rows_number, cols_number). """
    for row in table.iterrows():
        return row['version'], row['n_rows'], row['n_cols']
    return (None, 0, 0)
