# -*- coding: utf-8 -*-
"""
Writing data to a partition. The MPF file format is a conversion format that stores tabular data in rows and associates
it with metadata

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import datetime
import time
import gzip

import msgpack
import struct

def new_mpr(fs, path, stats=None):
    from os.path import split, splitext

    assert bool(fs)

    dn, file_ext = split(path)
    fn, ext = splitext(file_ext)

    if fs and not fs.exists(dn):
        fs.makedir(dn, recursive=True)

    if not ext:
        ext = '.msg'

    return MPRowsFile(fs, path)

class MPRError(Exception):
    pass

class GzipFile(gzip.GzipFile):
    """A Hacked GzipFile that will read only one gzip member and properly handle extra data afterward"""

    def __init__(self, filename=None, mode=None, compresslevel=9, fileobj=None, mtime=None, end_of_data=None):
        super(GzipFile, self).__init__(filename, mode, compresslevel, fileobj, mtime)
        self._end_of_data = end_of_data

    def _read(self, size=1024):
        """Alters the _read method to stop reading new gzip members when we've reached the end of the row data. """

        if self._new_member and self._end_of_data and self.fileobj.tell() >= self._end_of_data:
            raise EOFError, "Reached EOF"
        else:
            return super(GzipFile, self)._read(size)


class MPRowsFile(object):
    """The Message Pack Rows File format holds a collection of arrays, in message pack format, along with a
    dictionary of values. The format is designed for holding tabular data in an efficient, compressed form,
    and for associating it with metadata. """

    EXTENSION = '.msg'
    VERSION = 1
    MAGIC = 'AMBRMPDF'

    # 8s: Magic Number, H: Version,  I: Number of rows, I: number of columns
    # Q: Position of end of rows / Start of meta,
    # i: Header row, I: Data start row, I: Data end row
    FILE_HEADER_FORMAT = struct.Struct('>8sHIIQiII')
    FILE_HEADER_FORMAT_SIZE = FILE_HEADER_FORMAT.size

    META_TEMPLATE = {

        'schema': {},
        'stats': {},
        'types': {},
        'geo':{
            'srs': None,
            'bb': None
        },
        'excel':{
            'datemode': None,
            'worksheet': None
        },
        'source':{
            'url': None,
            'fetch_time': None,
            'file_type': None,
            'url_type': None,
            'inner_file': None
        },
        'row_spec':{
            'header_rows': None,
            'comment_rows': None,
            'start_row': None,
            'end_row': None,
            'data_pattern': None
        },
        'comments':{
            'header': None,
            'footer': None
        }
    }

    SCHEMA_TEMPLATE = {
        'pos': None,
        'name': None,
        'type': None,
        'description': None
    }

    def __init__(self,  url_or_fs, path=None):
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

        self._compress = True


    @property
    def path(self):
        return self._path

    @property
    def munged_path(self):
        if self._path.endswith(self.EXTENSION):
            return self._path
        else:
            return self._path + self.EXTENSION

    @staticmethod
    def encode_obj(obj):
        if isinstance(obj, datetime.datetime):
            return {'__datetime__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.date):
            return {'__date__': True, 'as_str': obj.isoformat()}
        elif isinstance(obj, datetime.time):
            return {'__time__': True, 'as_str': obj.strftime("%H:%M:%S")}
        elif hasattr(obj, 'render'):
            return obj.render()
        elif hasattr(obj, '__str__'):
            return str(obj)
        else:
            raise Exception("Unknown type on encode: {}, {}".format(type(obj), obj))


    @staticmethod
    def decode_obj(obj):

        if b'__datetime__' in obj:
            try:
                obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                # The preferred format is without the microseconds, but there are some lingering
                # bundle that still have it.
                obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%dT%H:%M:%S.%f")
        elif b'__time__' in obj:
            obj = datetime.time(*list(time.strptime(obj["as_str"], "%H:%M:%S"))[3:6])
        elif b'__date__' in obj:
            obj = datetime.datetime.strptime(obj["as_str"], "%Y-%m-%d").date()
        else:
            raise Exception("Unknown type on decode: {} ".format(obj))

        return obj

    @classmethod
    def read_file_header(cls, o, fh):
        o.magic, o.version, o.n_rows, o.n_cols, o.meta_start, o.header_row, o.data_start_row, o.data_end_row = \
            cls.FILE_HEADER_FORMAT.unpack(fh.read(cls.FILE_HEADER_FORMAT_SIZE))

    @classmethod
    def write_file_header(cls, o, fh):
        """Write the magic number, version and the file_header dictionary.  """

        hdf = cls.FILE_HEADER_FORMAT.pack(cls.MAGIC, cls.VERSION, o.n_rows, o.n_cols, o.meta_start,
                                          o.header_row, o.data_start_row, o.data_end_row)

        assert len(hdf) == cls.FILE_HEADER_FORMAT_SIZE

        fh.seek(0)

        fh.write(hdf)

        assert fh.tell() == cls.FILE_HEADER_FORMAT_SIZE, (fh.tell(), cls.FILE_HEADER_FORMAT_SIZE)

    @classmethod
    def read_meta(cls,o,fh):

        pos = fh.tell()

        fh.seek(o.meta_start)

        # Using the _fh b/c I suspect that the GzipFile attached to self._zfh has state that would
        # get screwed up if you read from a new position

        data = fh.read()

        if data:

            meta = msgpack.unpackb(data.decode('zlib'), encoding='utf-8')

        else:
            meta = {}

        fh.seek(pos)

        return meta

    @classmethod
    def write_meta(cls, o, fh):

        fh.seek(o.meta_start)  # Should probably already be there.

        fhb = msgpack.packb(o.meta, encoding='utf-8').encode('zlib')
        fh.write(fhb)

    @classmethod
    def info(self, o):

        return dict(
            version=o.version,
            rows=o.n_rows,
            cols=o.n_cols,
            header_row = o.header_row,
            data_start_row = o.data_start_row,
            data_end_row=o.data_end_row,
            data_start_pos = o.data_start,
            meta_start_pos = o.meta_start)

    def load_rows(self, source, intuit_rows = True, intuit_type = True):

        from .intuit import RowIntuiter, TypeIntuiter

        with self.writer as w:
            w.load_rows(source)
            w.close()

        if intuit_rows:
            with self.reader as r:
                ri = RowIntuiter().run(r.raw)

            with self.writer as w:
                w.set_row_spec(ri)

        if intuit_type:
            with self.reader as r:
                ti = TypeIntuiter().process_header(r.headers).run(r.rows)

            with self.writer as w:
                w.set_types(ti)

        return self

    @property
    def reader(self):
        if not self._reader:
            self._reader = MPRReader(self, self._fs.open(self.munged_path, mode='rb'), compress = self._compress)

        return self._reader

    @property
    def writer(self):
        if not self._writer:
            if self._fs.exists(self.munged_path):
                mode = 'r+b'
            else:
                mode = 'wb'

            self._writer = MPRWriter(self, self._fs.open(self.munged_path, mode=mode), compress = self._compress)

        return self._writer


class MPRWriter(object):

    MAGIC = MPRowsFile.MAGIC
    VERSION = MPRowsFile.VERSION
    FILE_HEADER_FORMAT = MPRowsFile.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = MPRowsFile.FILE_HEADER_FORMAT.size
    META_TEMPLATE = MPRowsFile.META_TEMPLATE
    SCHEMA_TEMPLATE = MPRowsFile.SCHEMA_TEMPLATE

    def __init__(self, parent, fh, compress = True):

        from copy import deepcopy

        assert fh

        self.parent = parent
        self._fh = fh
        self._compress = compress

        self._zfh = None # Compressor for writing rows
        self.version = self.VERSION
        self.magic = self.MAGIC
        self.data_start = self.FILE_HEADER_FORMAT_SIZE
        self.meta_start = 0
        self.header_row = -1 # -1 means  no header
        self.data_start_row = 0
        self.data_end_row = 0

        self.n_rows = 0
        self.n_cols = 0

        self._row_writer = None

        try:

            MPRowsFile.read_file_header(self, self._fh)

            self._fh.seek(self.meta_start)

            data = self._fh.read()

            self.meta = msgpack.unpackb(data.decode('zlib'), encoding='utf-8')

            self._fh.seek(self.meta_start)

        except IOError:
            self._fh.seek(0)

            self.meta_start = self.data_start

            self.meta = deepcopy(self.META_TEMPLATE)

            self.write_file_header() # Get moved to the start of row data.

        # Creating the GzipFile object will also write the Gzip header, about 21 bytes of data.
        if self._compress:
            self._zfh = GzipFile(fileobj=self._fh)  # Compressor for writing rows
        else:
            self._zfh = self._fh

        self._row_writer = lambda row: self._zfh.write(
            msgpack.packb(row, default=MPRowsFile.encode_obj, encoding='utf-8'))

    @property
    def info(self):
        return MPRowsFile.info(self)

    def set_schema(self, headers):
        from copy import deepcopy

        schema = []

        for i, h in enumerate(headers):
            d = deepcopy(self.SCHEMA_TEMPLATE)

            if isinstance(h, dict):
                d = dict(h.items())
                d['pos'] = i
                schema.append(d)
            else:
                d['pos'] = i
                d['name'] = h
                schema.append(d)

        self.meta['schema'] = schema

    def insert_headers(self, headers):
        self.set_schema(headers)
        self.header_row = 0
        self.data_start_row = 1
        self._row_writer([ c['name'] for c in self.meta['schema']] )

    def insert_row(self, row):

        self.n_rows += 1
        self.n_cols = max(self.n_cols, len(row))
        self.data_end_row = self.n_rows

        self._row_writer(row)

    def load_rows(self, source, first_is_header = False):
        """Load rows from an iterator"""
        from itertools import imap

        itr = iter(source)

        for i, row in enumerate(iter(source)):

            if first_is_header and i == 0:
                self.insert_headers(next(itr))
                continue
            else:
                self.insert_row(next(itr))


    def close(self):

        if self._fh:
            # First close the Gzip file, so it can flush, etc.

            if self._compress and self._zfh:
                self._zfh.close()

            self._zfh = None

            self.meta_start = self._fh.tell()

            self.write_file_header()
            self._fh.seek(self.meta_start)

            self.write_meta()

            self._fh.close()
            self._fh = None

            if self.parent:
                self.parent._writer = None


    def write_file_header(self):
        """Write the magic number, version and the file_header dictionary.  """
        MPRowsFile.write_file_header(self, self._fh)

    def write_meta(self):
        MPRowsFile.write_meta(self, self._fh)

    def set_source_spec(self, spec):

        ms = self.meta['source']

        ms['url']= spec.url
        ms['fetch_time']= spec.download_time
        ms['file_type']= spec.filetype
        ms['url_type'] = spec.urltype

        me = self.meta['excel']
        me['workbook'] = spec.segment

        rs = self.meta['row_spec']

        if spec.header_lines:
            rs['header_rows'] = spec.header_lines

        if spec.start_line is not None and spec.start_line != '':
            rs['start_row'] = spec.start_line

        if spec.end_line is not None and spec.end_line != '':
            rs['end_row'] = spec.end_line

    def set_types(self, ti):
        """Set Types from a type intuiter object"""

        if self.meta['schema']:
            results = {r['position']: r for r in ti._dump()}
            for i, row in enumerate(self.meta['schema']):
                result = results[i]
                assert result['header'] == row['name']
                del result['position']


                if not row.get('type'):
                    result['type'] = result['resolved_type']

                row.update(result)

        else:
            schema = []
            for i, r in enumerate(ti._dump()):
                r['pos'] = r['position']
                r['name'] = r['header']
                r['type'] = r['resolved_type']
                del r['position']
                del r['header']
                schema.append(r)

            self.meta['schema'] = schema

    def set_stats(self, stats):
        pass

    def set_row_spec(self, ri):
        """Set the row spec and schema from a RowIntuiter object"""
        from itertools import islice
        from ambry_sources.intuit import RowIntuiter
        import re

        w = self.parent.writer

        w.data_start_row = ri.start_line

        w.meta['row_spec']['header_rows'] = ri.header_lines
        w.meta['row_spec']['comment_rows'] = ri.comment_lines
        w.meta['row_spec']['start_row'] = ri.start_line
        w.meta['row_spec']['end_row'] = ri.end_line
        w.meta['row_spec']['data_pattern'] = ri.data_pattern_source

        mangler = lambda name: re.sub('_+', '_', re.sub('[^\w_]', '_', name).lower()).rstrip('_')

        schema = []
        for i, h in enumerate(ri.headers):
            d = dict(
                pos=i,
                name=mangler(h),
                description=h
            )

            schema.append(d)

        w.meta['schema'] = schema
        w.close()

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


class MPRReader(object):
    """
    Read an MPR file

    """
    MAGIC = MPRowsFile.MAGIC
    VERSION = MPRowsFile.VERSION
    FILE_HEADER_FORMAT = MPRowsFile.FILE_HEADER_FORMAT
    FILE_HEADER_FORMAT_SIZE = MPRowsFile.FILE_HEADER_FORMAT.size
    META_TEMPLATE = MPRowsFile.META_TEMPLATE
    SCHEMA_TEMPLATE = MPRowsFile.SCHEMA_TEMPLATE

    def __init__(self, parent, fh, compress = True):
        """Reads the file_header and prepares for iterating over rows"""

        self.parent = parent
        self._fh = fh
        self._compress = compress
        self._headers = None
        self.data_start = 0
        self.meta_start = 0
        self.header_row = -1  # -1 means no header, 0 == first row.
        self.data_start_row = 0
        self.data_end_row = 0

        self.pos = 0 # Row position for next read, starts at 1, since header is always 0

        self.n_rows = 0
        self.n_cols = 0

        self._in_iteration = False

        MPRowsFile.read_file_header(self, self._fh)

        self.data_start = int(self._fh.tell())

        assert self.data_start == self.FILE_HEADER_FORMAT_SIZE

        if self._compress:
            self._zfh = GzipFile(fileobj=self._fh, end_of_data=self.meta_start)
        else:
            self._zfh =self._fh

        self.unpacker = msgpack.Unpacker(self._zfh, object_hook=MPRowsFile.decode_obj, encoding='utf-8')

        self._meta = None

    @property
    def info(self):
        return MPRowsFile.info(self)

    @property
    def meta(self):

        if self._meta is None:

            # Using the _fh b/c I suspect that the GzipFile attached to self._zfh has state that would
            # get screwed up if you read from a new position
            self._meta = MPRowsFile.read_meta(self, self._fh)

        return self._meta

    @property
    def headers(self):
        """Return the headers row, which can come from one of three locations.

        - Normally, the header is the first row in the data
        - The header row may be specified as a later row with ``header_row``, in which case all of the rows before
            it are considered comments and are not returned.
        - If it is not specified as the first row, it can be given in the metadata schema
        - If there is no first row and no schema, a header is generated from column numbers

        The header always exists, and it is always returned as row 0, except when using the raw iterator.

        """
        from itertools import islice

        if not self._headers:

            if self._in_iteration:
                raise MPRError("Can't get header because iteration has already started")

            assert self.pos == 0

            assert self.header_row == 0 or self.header_row == -1, self.header_row

            if self.header_row == 0:
                self._headers = self.unpacker.next()
                self.pos += 1

            elif self.meta['schema']:
                # No header row exists, so try to get one from the schema
                self._headers = [c['name'] for c in self.meta['schema']]

            else:
                # No schema, so just return numbered columns
                self._headers = [ 'col'+str(e) for e in range(0,self.n_cols)]

        return self._headers

    def consume_to_data(self):
        """Read and discard rows until we get to the data start row"""
        from itertools import islice

        if  self.pos >= self.data_start_row:
            return

        while self.pos != self.data_start_row:
            next(self.unpacker)
            self.pos += 1

        return


    @property
    def raw(self):
        """A raw iterator, which ignores the data start and stop rows and returns all rows, as rows"""
        from ambry_sources.sources import RowProxy

        self._fh.seek(self.data_start)

        try:
            self._in_iteration = True

            for i, row in enumerate(self.unpacker):
                yield row
                self.pos += 1

        finally:
            self._in_iteration = False
            self.close()

    @property
    def meta_raw(self):
        import sys
        """self self.raw interator, but returns a tuple with the rows classified"""

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
        """Iterator for reading rows"""
        from ambry_sources.sources import RowProxy

        self._fh.seek(self.data_start)

        _ = self.headers # Get the header, but don't return it.

        self.consume_to_data()

        try:
            self._in_iteration = True

            for i in range(self.data_start_row, self.data_end_row+1):
                yield next(self.unpacker)
                self.pos+=1

        except:
            self._in_iteration = False



    def __iter__(self):
        """Iterator for reading rows as RowProxy objects"""
        from ambry_sources.sources import RowProxy

        self._fh.seek(self.data_start)

        rp = RowProxy(self.headers)

        self.consume_to_data()

        try:
            self._in_iteration = True
            for i in range(self.data_start_row, self.data_end_row + 1):
                yield rp.set_row(next(self.unpacker))
                self.pos += 1

        except:
            self._in_iteration = False

    def close(self):
        if self._fh:
            self.meta # In case caller wants to read mea after close.
            self._fh.close()
            self._fh = None
            if self.parent:
                self.parent._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

        if exc_val:
            return False



