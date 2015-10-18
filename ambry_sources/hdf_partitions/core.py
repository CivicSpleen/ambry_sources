# -*- coding: utf-8 -*-
"""
Writing data to a HDF partition.
"""

import datetime
from functools import reduce
import struct
import time
import zlib

from tables import open_file, StringCol, Int64Col, Float64Col, BoolCol

import six
from six import string_types, iteritems, text_type

import msgpack

from ambry_sources.sources import RowProxy

# FIXME: rename to H5 - H5Partition, H5Writer


class MPRError(Exception):
    pass


class HDFPartition(object):
    """ FIXME: """

    EXTENSION = '.mpr'
    VERSION = 1
    MAGIC = 'AMBRMPDF'

    # 8s: Magic Number, H: Version,  I: Number of rows, I: number of columns
    # Q: Position of end of rows / Start of meta,
    # I: Data start row, I: Data end row
    FILE_HEADER_FORMAT = struct.Struct('>8sHIIQII')

    FILE_HEADER_FORMAT_SIZE = FILE_HEADER_FORMAT.size

    # These are all of the keys for the  schema. The schema is a collection of rows, with these
    # keys being the first, followed by one row per column.
    SCHEMA_TEMPLATE = [
        'pos',
        'name',
        'type',
        'description',
        'start',
        'width',

        # types
        'position',
        'header',
        'length',
        'has_codes',
        'type_count',  # Note! Row Intuiter object call this 'count'

        'ints',
        'floats',
        'strs',
        'unicode',
        'nones',
        'datetimes',
        'dates',
        'times',
        'strvals',

        # Stats
        'flags',
        'lom',
        'resolved_type',
        'stat_count',  # Note! Stat object calls this 'count'
        'nuniques',
        'mean',
        'std',
        'min',
        'p25',
        'p50',
        'p75',
        'max',
        'skewness',
        'kurtosis',
        'hist',
        'text_hist',
        'uvalues']

    META_TEMPLATE = {

        'schema': [SCHEMA_TEMPLATE],
        'about': {
            'create_time': None,  # Timestamp when file was  created.
            'load_time': None  # Length of time MPRowsFile.load_rows ran, in seconds()
        },
        'geo': {
            'srs': None,
            'bb': None
        },
        'excel': {
            'datemode': None,
            'worksheet': None
        },
        'source': {
            'url': None,
            'fetch_time': None,
            'file_type': None,
            'url_type': None,
            'inner_file': None,
            'encoding': None
        },
        'row_spec': {
            'header_rows': None,
            'comment_rows': None,
            'start_row': None,
            'end_row': None,
            'data_pattern': None
        },
        'comments': {
            'header': None,
            'footer': None
        }
    }

    def __init__(self, url_or_fs, path=None):
        """

        :param url_or_fs:
        :param path:
        :return:
        """

        from fs.opener import opener

        if path:
            self._fs, self._path = url_or_fs, path
        else:
            self._fs, self._path = opener.parse(url_or_fs)

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

    @staticmethod
    def encode_obj(obj):

        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.date):
            return {'__date__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.time):
            return {'__time__': True, 'as_str': obj.strftime('%H:%M:%S')}
        elif hasattr(obj, 'render'):
            return obj.render()
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            raise Exception('Unknown type on encode: {}, {}'.format(type(obj), obj))

    @staticmethod
    def decode_obj(obj):

        if '__datetime__' in obj:
            try:
                obj = datetime.datetime.strptime(obj['as_str'], '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.datetime.strptime(obj['as_str'], '%Y-%m-%dT%H:%M:%S.%f')
        elif '__time__' in obj:
            obj = datetime.time(*list(time.strptime(obj['as_str'], '%H:%M:%S'))[3:6])
        elif '__date__' in obj:
            obj = datetime.datetime.strptime(obj['as_str'], '%Y-%m-%d').date()
        else:
            raise Exception('Unknown type on decode: {} '.format(obj))

        return obj

    @classmethod
    def read_file_header(cls, o, fh):
        try:
            o.magic, o.version, o.n_rows, o.n_cols, o.meta_start, o.data_start_row, o.data_end_row = \
                cls.FILE_HEADER_FORMAT.unpack(fh.read(cls.FILE_HEADER_FORMAT_SIZE))
        except struct.error as e:
            raise IOError("Failed to read file header; {}; path = {}".format(e, o.parent.path))

    @classmethod
    def write_file_header(cls, o, fh):
        """Write the magic number, version and the file_header dictionary.  """

        int(o.data_start_row)
        magic = cls.MAGIC
        if isinstance(magic, text_type):
            magic = magic.encode('utf-8')

        hdf = cls.FILE_HEADER_FORMAT.pack(magic, cls.VERSION, o.n_rows, o.n_cols, o.meta_start,
                                          o.data_start_row,  o.data_end_row if o.data_end_row else o.n_rows)

        assert len(hdf) == cls.FILE_HEADER_FORMAT_SIZE

        fh.seek(0)

        fh.write(hdf)

        assert fh.tell() == cls.FILE_HEADER_FORMAT_SIZE, (fh.tell(), cls.FILE_HEADER_FORMAT_SIZE)

    @classmethod
    def read_meta(cls, o, fh):
        # FIXME: Deprecated. Use self.reader.meta instead.
        raise Exception('Deprecated')

    @classmethod
    def _columns(cls, o, n_cols=0):
        s = o.meta['schema']

        assert len(s) >= 1  # Should always have header row.
        assert o.meta['schema'][0] == HDFPartition.SCHEMA_TEMPLATE, (o.meta['schema'][0], HDFPartition.SCHEMA_TEMPLATE)

        # n_cols here is for columns in the data table, which are rows in the headers table
        n_cols = max(n_cols, o.n_cols, len(s)-1)

        for i in range(1, n_cols+1):
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

        assert o.meta['schema'][0] == HDFPartition.SCHEMA_TEMPLATE

    @property
    def info(self):
        return self._info(self.reader)

    @classmethod
    def _info(cls, o):

        return dict(
            version=o.version,
            data_start_pos=o.data_start,
            meta_start_pos=o.meta_start,
            rows=o.n_rows,
            cols=o.n_cols,
            header_rows=o.meta['row_spec']['header_rows'],
            data_start_row=o.data_start_row,
            data_end_row=o.data_end_row,
            comment_rows=o.meta['row_spec']['comment_rows'],
            headers=o.headers
        )

    @property
    def exists(self):
        return self._fs.exists(self.path)

    def remove(self):
        if self.exists:
            self._fs.remove(self.path)

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

    def run_type_intuiter(self):
        """Run the Type Intuiter and store the results back into the metadata"""
        from ambry_sources.intuit import TypeIntuiter

        try:
            self._process = 'intuit_type'
            self._start_time = time.time()

            with self.reader as r:
                ti = TypeIntuiter().process_header(r.headers).run(r.rows, r.n_rows)

            with self.writer as w:
                w.set_types(ti)
        finally:
            self._process = 'none'

    def run_row_intuiter(self):
        """Run the row intuiter and store the results back into the metadata"""
        from ambry_sources.intuit import RowIntuiter

        try:
            self._process = 'intuit_rows'
            self._start_time = time.time()

            with self.reader as r:
                ri = RowIntuiter().run(r.raw, r.n_rows)

            with self.writer as w:
                w.set_row_spec(ri)

        finally:
            self._process = 'none'

    def run_stats(self):
        """Run the stats process and store the results back in the metadata"""
        from ambry_sources.stats import Stats

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

    def load_rows(self, source,  spec=None, intuit_rows=None, intuit_type=True, run_stats=True):
        try:

            # The spec should always be part of the source
            assert spec is None

            self._load_rows(source,
                            intuit_rows=intuit_rows,
                            intuit_type=intuit_type, run_stats=run_stats)
        except:
            raise
            self.writer.close()
            self.remove()
            raise

        return self

    def _load_rows(self, source,  intuit_rows=None, intuit_type=True, run_stats=True):
        from ambry_sources.exceptions import RowIntuitError
        if self.n_rows:
            raise MPRError("Can't load_rows; rows already loaded. n_rows = {}".format(self.n_rows))

        spec = getattr(source, 'spec', None)

        # None means to determine True or False from the existence of a row spec
        if intuit_rows is None:

            if spec is None:
                intuit_rows = True
            elif spec.has_rowspec:
                intuit_rows = False
            else:
                intuit_rows = True

        try:

            self._process = 'load_rows'
            self._start_time = time.time()

            with self.writer as w:

                w.load_rows(source)

                if spec:
                    w.set_source_spec(spec)

            if intuit_rows:
                try:
                    self.run_row_intuiter()
                except RowIntuitError:
                    # FIXME Need to report this, but there is currently no way to get
                    # the higher level logger.
                    pass

            elif spec:

                with self.writer as w:
                    w.set_row_spec(spec)
                    assert w.meta['schema'][0] == HDFPartition.SCHEMA_TEMPLATE

            if intuit_type:
                self.run_type_intuiter()

            if run_stats:
                self.run_stats()

            with self.writer as w:

                if not w.data_end_row:
                    w.data_end_row = w.n_rows

        finally:
            self._process = None

        return self

    def open(self,  mode='rb'):
        return self._fs.open(self.path, mode=mode)

    @property
    def reader(self):
        if not self._reader:
            self._reader = HDFReader(self, self._fs.open(self.path, mode='rb'))
        return self._reader

    def __iter__(self):
        """Iterate over a reader"""

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
        from os.path import dirname
        if not self._writer:
            self._process = 'write'
            if self._fs.exists(self.path):
                mode = 'r+b'
            else:
                mode = 'wb'

            if not self._fs.exists(dirname(self.path)):
                self._fs.makedir(dirname(self.path), recursive=True)

            self._writer = HDFWriter(self, self._fs.open(self.path, mode=mode))

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


class HDFWriter(object):

    MAGIC = HDFPartition.MAGIC
    VERSION = HDFPartition.VERSION
    FILE_HEADER_FORMAT = HDFPartition.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = HDFPartition.FILE_HEADER_FORMAT.size
    META_TEMPLATE = HDFPartition.META_TEMPLATE
    SCHEMA_TEMPLATE = HDFPartition.SCHEMA_TEMPLATE

    def __init__(self, parent, filename):
        from copy import deepcopy
        import re

        if not isinstance(filename, string_types):
            # FIXME: add tests
            raise ValueError(
                'Pytables requires string with filename. Got {} instead.'
                .format(filename.__class__))

        self.parent = parent
        self.version = self.VERSION
        self.magic = self.MAGIC
        self.data_start = self.FILE_HEADER_FORMAT_SIZE
        self.meta_start = 0
        self.data_start_row = 0
        self.data_end_row = None

        self.n_rows = 0
        self.n_cols = 0

        self.cache = []

        try:
            # Try to read an existing file
            # FIXME:
            # HDFPartition.read_file_header(self, self._h5_file)
            raise IOError
            self._h5_file = open_file(filename, mode='a')

            self._h5_file.seek(self.meta_start)

            data = self._h5_file.read()

            self.meta = msgpack.unpackb(zlib.decompress(data), encoding='utf-8')

            self._h5_file.seek(self.meta_start)
            self._is_new = False

        except IOError:
            # No, doesn't exist
            self._h5_file = open_file(filename, mode='w')

            self.meta_start = self.data_start

            self.meta = deepcopy(self.META_TEMPLATE)

            # self.write_file_header()  # Get moved to the start of row data.
            self._is_new = True

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
        """Set column names"""

        if not headers:
            return

        assert isinstance(headers,  (tuple, list)), headers

        for i, row in enumerate(HDFPartition._columns(self, len(headers))):
            assert isinstance(headers[i], string_types)
            row.name = headers[i]

        assert self.meta['schema'][0] == HDFPartition.SCHEMA_TEMPLATE

    @property
    def columns(self):
        """ Returns the headers rows. """
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

    def _write_rows(self, rows=None):
        if self._is_new:
            # partition_group = self._h5_file.create_group('/', 'partition', 'Meta information')
            self.write_meta()

            # create column descriptor
            rows_descriptor = _get_rows_descriptor(self.columns)

            self._h5_file.create_table(
                '/partition', 'rows', rows_descriptor, 'Rows (data) of the partition.',
                createparents=True)

        rows, clear_cache = (self.cache, True) if not rows else (rows, False)

        if not rows:
            return

        rows_table = self._h5_file.root.partition.rows
        partition_row = rows_table.row
        for row in rows:
            partition_row['col1'] = row[0]
            partition_row['col2'] = row[1]
            partition_row.append()
        rows_table.flush()

        # Hope that the max # of cols is found in the first 100 rows
        # FIXME! This won't work if rows is an interator.
        self.n_cols = reduce(max, (len(e) for e in rows[:100]), self.n_cols)

        if clear_cache:
            self.cache = []

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
        """Load rows from an iterator"""

        for row in iter(source):
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
            # self.write_file_header()
            # self._h5_file.seek(self.meta_start)
            # FIXME: Write meta.
            # self.write_meta()
            self._h5_file.close()
            self._h5_file = None

            if self.parent:
                self.parent._writer = None

    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """
        HDFPartition.write_file_header(self, self._h5_file)

    def write_meta(self):
        assert self.meta['schema'][0] == HDFPartition.SCHEMA_TEMPLATE
        if self._is_new:
            self._h5_file.create_group(
                '/partition', 'meta', 'Meta information of the partition.',
                createparents=True)
            self._save_about(create=True)
            self._save_comments(create=True)
            self._save_excel(create=True)
            self._save_geo(create=True)
            self._save_row_spec(create=True)
            self._save_schema(create=True)
            self._save_source(create=True)

    def _save_meta_child(self, child, descriptor, create=False):
        if child == 'schema':
            # Special case - should not include first line.
            print('hey')
            return
        if create:
            self._h5_file.create_table(
                '/partition/meta', child,
                descriptor, 'meta.{}'.format(child),
                createparents=True)
        table = getattr(self._h5_file.root.partition.meta, child)
        row = table.row
        for k, v in self.meta[child].items():
            row[k] = v or _get_default(descriptor[k].__class__)  # FIXME: what about dflt (default) of the field
        row.append()
        table.flush()

    def _save_about(self, create=False):
        descriptor = {
            'load_time': Float64Col(),
            'create_time': Float64Col()
        }
        self._save_meta_child('about', descriptor, create=create)

    def _save_schema(self, create=False):
        # FIXME: do we really need to store schema? Try to retrieve it from file.
        descriptor = {
            'pos': Int64Col(),
            'name': StringCol(itemsize=255),
            'type': StringCol(itemsize=255),
            'description': StringCol(itemsize=1024),
            'start': Int64Col(),  # FIXME: Ask Eric about type.
            'width': Int64Col(),
            'position': Int64Col(),
            'header': Int64Col(),
            'length': Int64Col(),
            'has_codes': BoolCol(),
            'type_count': Int64Col(),
            'ints': Int64Col(),
            'floats': Int64Col(),
            'strs': Int64Col(),
            'unicode': Int64Col(),
            'nones': Int64Col(),
            'datetimes': Int64Col(),
            'dates': Int64Col(),
            'times': Int64Col(),
            'strvals': Int64Col(),
            'flags': Int64Col(),  # FIXME: Ask Eric about type.
            'lom': Int64Col(),  # FIXME: Ask Eric about type.
            'resolved_type': StringCol(itemsize=40),
            'stat_count': Int64Col(),
            'nuniques': Int64Col(),
            'mean': Float64Col(),
            'std': Float64Col(),
            'min': Float64Col(),
            'p25': Float64Col(),
            'p50': Float64Col(),
            'p75': Float64Col(),
            'max': Float64Col(),
            'skewness': Float64Col(),  # Ask Eric about type.
            'kurtosis': Float64Col(),  # Ask Eric about type.
            'hist': Float64Col(),  # Ask Eric about type.
            'text_hist': StringCol(itemsize=255),
            'uvalues': StringCol(itemsize=255)  # Ask Eric about type.
        }
        self._save_meta_child('schema', descriptor, create=create)

    def _save_excel(self, create=False):
        descriptor = {
            'worksheet': StringCol(itemsize=255),
            'datemode': StringCol(itemsize=255)  # FIXME: Check datemode again. Is it string?
        }
        self._save_meta_child('excel', descriptor, create=create)

    def _save_comments(self, create=False):
        # FIXME: do we really need to store header and footer? HDF can contains rows only.
        descriptor = {
            'header': StringCol(itemsize=255),
            'footer': StringCol(itemsize=255)
        }
        self._save_meta_child('comments', descriptor, create=create)

    def _save_source(self, create=False):
        descriptor = {
            'fetch_time': Float64Col(),
            'encoding': StringCol(itemsize=255),
            'url': StringCol(itemsize=1024),  # FIXME: Ask Eric about max length of the url.
            'file_type': StringCol(itemsize=50),
            'inner_file': StringCol(itemsize=255),  # FIXME: Ask Eric about length.
            'url_type': StringCol(itemsize=255),  # FIXME: Ask Eric about length.
        }
        self._save_meta_child('source', descriptor, create=create)

    def _save_row_spec(self, create=False):
        descriptor = {
            'end_row': Int64Col(),
            'header_rows': Int64Col(),
            'start_row': Int64Col(),
            'comment_rows': Int64Col(),
            'data_pattern': StringCol(itemsize=255)  # FIXME: Ask Eric about size.
        }

        self._save_meta_child('row_spec', descriptor, create=create)

    def _save_geo(self, create=False):
        descriptor = {
            'srs': Int64Col(),  # FIXME: Ask Eric about type.
            'bb': Int64Col(),  # FIXME: Ask Eric about type.
        }
        self._save_meta_child('geo', descriptor, create=create)

    def set_types(self, ti):
        """Set Types from a type intuiter object"""

        results = {int(r['position']): r for r in ti._dump()}
        for i in range(len(results)):

            for k, v in iteritems(results[i]):
                k = {'count': 'type_count'}.get(k, k)
                self.column(i + 1)[k] = v

            if not self.column(i + 1).type:
                self.column(i + 1).type = results[i]['resolved_type']

    def set_stats(self, stats):
        """Copy stats into the schema"""

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
        me['workbook'] = spec.segment

        if spec.columns:

            for i, sc in enumerate(spec.columns, 1):
                c = self.column(i)

                if c.name:
                    assert sc.name == c.name

                c.start = sc.start
                c.width = sc.width

    def set_row_spec(self, ri_or_ss):
        """Set the row spec and schema from a RowIntuiter object or a SourceSpec"""

        from itertools import islice
        from operator import itemgetter
        from ambry_sources.intuit import RowIntuiter

        if isinstance(ri_or_ss, RowIntuiter):
            ri = ri_or_ss

            with self.parent.writer as w:

                w.data_start_row = ri.start_line
                w.data_end_row = ri.end_line if ri.end_line else None

                w.meta['row_spec']['header_rows'] = ri.header_lines
                w.meta['row_spec']['comment_rows'] = ri.comment_lines
                w.meta['row_spec']['start_row'] = ri.start_line
                w.meta['row_spec']['end_row'] = ri.end_line
                w.meta['row_spec']['data_pattern'] = ri.data_pattern_source

                w.headers = [self.header_mangler(h) for h in ri.headers]

        else:
            ss = ri_or_ss

            with self.parent.reader as r:
                # If the header lines are specified, we need to also coalesce them ad
                # set the header
                if ss.header_lines:

                    max_header_line = max(ss.header_lines)
                    rows = list(islice(r.raw, max_header_line + 1))

                    header_lines = itemgetter(*ss.header_lines)(rows)

                    if not isinstance(header_lines[0], (list, tuple)):
                        header_lines = [header_lines]

                else:
                    header_lines = None

            with self.parent.writer as w:

                w.data_start_row = ss.start_line
                w.data_end_row = ss.end_line if ss.end_line else None

                w.meta['row_spec']['header_rows'] = ss.header_lines
                w.meta['row_spec']['comment_rows'] = None
                w.meta['row_spec']['start_row'] = ss.start_line
                w.meta['row_spec']['end_row'] = ss.end_line
                w.meta['row_spec']['data_pattern'] = None

                if header_lines:
                    w.headers = [self.header_mangler(h) for h in RowIntuiter.coalesce_headers(header_lines)]

        # Now, look for the end line.
        if False:
            # FIXME: Maybe later ...
            r = self.parent.reader
            # Look at the last 100 rows, but don't start before the start row.
            test_rows = 100
            start = max(r.data_start_row, r.data_end_row - test_rows)

            end_rows = list(islice(r.raw, start, None))

            ri.find_end(end_rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False


class HDFReader(object):
    """ Read an h5 file. """

    def __init__(self, parent, filename):
        """Reads the file_header and prepares for iterating over rows"""

        if not isinstance(filename, string_types):
            # This is the pytables constraint.
            raise ValueError(
                'HDFReader requires string with filename. Got {} instead.'
                .format(filename.__class__))

        self.parent = parent
        self._h5_file = open_file(filename, mode='r')
        self._headers = None
        self.data_start = 0
        self.meta_start = 0
        self.data_start_row = 0
        self.data_end_row = 0

        self.pos = 0  # Row position for next read, starts at 1, since header is always 0

        self.n_rows = 0
        self.n_cols = 0

        self._in_iteration = False

        # FIXME: seems useless
        # HDFPartition.read_file_header(self, self._fh)

        self.data_start = 0  # FIXME: Seems useless because it's always 0.
        self._meta = None

    @property
    def info(self):
        # FIXME:
        return HDFPartition._info(self)

    @property
    def meta(self):
        if self._meta is None:
            self._meta = self._read_meta()
        return self._meta

    def _read_meta(self):
        # FIXME: move to the private methods.
        from copy import deepcopy
        meta = deepcopy(HDFPartition.META_TEMPLATE)
        for key, value in meta.items():
            saved_data = self._read_meta_child(key)
            if key == 'schema':
                # FIXME: Special case. Implement
                continue
            for k, default_value in value.items():
                meta[key][k] = saved_data.get(k, default_value)
        return meta

    def _read_meta_child(self, child):
        """ Reads first row from `child` table of h5 file and returns it.

        Args:
            child (str): name of the table from h5 file.

        Returns:
            dict:
        """
        table = getattr(self._h5_file.root.partition.meta, child)
        for row in table.iterrows():
            return {c: row[c] for c in table.colnames}
        return {}

    @property
    def columns(self):
        """ Returns columns specifications in the ambry_source format.

        Returns:
            list of FIXME:

        """
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
        # FIXME: Seems useless because start and stop rows are not used for HDF.
        try:
            self._in_iteration = True
            table = self._h5_file.root.partition.rows
            for row in table.iterrows():
                yield [row[c] for c in table.colnames]
                self.pos += 1
        finally:
            self._in_iteration = False
            self.close()

    @property
    def meta_raw(self):
        """self self.raw interator, but returns a tuple with the rows classified"""
        # FIXME: Seems useless.

        rs = self.meta['row_spec']

        hr = rs['header_rows'] or []
        cr = rs['comment_rows'] or []
        sr = rs['start_row'] or self.data_start_row
        er = rs['end_row'] or self.data_end_row

        for i, row in enumerate(self.raw):

            if i in hr:
                label = 'H'
            elif i in cr:
                label = 'C'
            elif sr <= i <= er:
                label = 'D'
            else:
                label = 'B'

            yield (i, self.pos, label), row

    @property
    def rows(self):
        """ Iterator for reading rows. """

        # it's exactly the same as raw iterator for HDF.
        return self.raw

    def __iter__(self):
        """ Iterator for reading rows as RowProxy objects

        WARNING: This routine returns RowProxy objects. RowProxy objects
            are reused, so if you construct a list directly from the output from this method,
            the list will have multiple copies of a single RowProxy, which will
            have as an inner row the last result row. If you will be directly constructing
            a list, use a getter that extracts the inner row, or which converted the RowProxy
            to a dict.

        """
        rp = RowProxy(self.headers)
        try:
            self._in_iteration = True
            table = self._h5_file.root.partition.rows
            for row in table.iterrows():
                r = [row[c] for c in table.colnames]
                yield rp.set_row(r)
                self.pos += 1
        finally:
            self._in_iteration = False

    def select(self, predicate=None, headers=None):
        """
        Select rows from the reader using a predicate to select rows and and itemgetter to return a
        subset of elements
        :param predicate: If defined, a callable that is called for each rowm and if it returns true, the
        row is included in the output.
        :param getter: If defined, a list or tuple of header names to return from each row

        Equivalent to:

            from itertools import imap, ifilter

            return imap(getter, ifilter(predicate, iter(self)))

        :return: iterable of results

        WARNING: This routine works from the reader iterator, which returns RowProxy objects. RowProxy objects
        are reused, so if you construct a list directly from the output from this method, the list will have
        multiple copies of a single RowProxy, which will have as an inner row the last result row. If you will
        be directly constructing a list, use a getter that extracts the inner row, or which
        converted the RowProxy to a dict:

            list(s.datafile.select(lambda r: r.stusab == 'CA', lambda r: r.dict))

        """

        if headers:

            from operator import itemgetter
            from .sources import RowProxy

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
            self.meta  # In case caller wants to read mea after close.
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


def _get_rows_descriptor(columns):
    """ Converts columns specifications from ambry_sources format to pytables descriptor.

    Args:
        columns FIXME: with example

    Returns:
        dict: FIXME: with example
    """
    # FIXME: Add tests.
    TYPE_MAP = {
        'int': Int64Col,
        'long': Int64Col,
        'str': lambda: StringCol(itemsize=255),  # FIXME: What is the size?
        'float': lambda: Float64Col(shape=(2, 3))
    }
    descriptor = {}

    for column in columns:
        pytables_type = TYPE_MAP.get(column['type'])
        if not pytables_type:
            raise Exception(
                'Failed to convert {} ambry_sources type to pytables type.'.format(column['type']))
        descriptor[column['name']] = pytables_type()
    return descriptor


def _get_default(pytables_type):
    """ Returns default value for given pytable type. """
    TYPE_MAP = {
        Int64Col: 0,
        Float64Col: 0.0,
        StringCol: ''
    }
    return TYPE_MAP[pytables_type]
