"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import logging

class SourceError(Exception):
    pass


class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
    def __init__(self, fs, path, mode='r', container = None,  account_accessor=None):

        self._fs = fs
        self._path = path
        self._mode = mode
        self._container = container
        self._account_accessor = account_accessor

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    def syspath(self):
        return self._fs.getsyspath(self._path)

    def source_pipe(self):
        return self._source.row_gen()

    def sub_cache(self):
        """Return a fs directory associated with this file """
        import os.path

        if self._container:
            fs, container_path = self._container

            dir_path = os.path.join(container_path + '_')

            fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return fs.opendir(dir_path)

        else:

            dir_path = os.path.join(self._path+'_')

            self._fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return self._fs.opendir(dir_path)

    @property
    def path(self):
        return self._path

    def __str__(self):

        from fs.errors import NoSysPathError

        try:
            return self.syspath()
        except NoSysPathError:
            return "Delayed Open: source = {}; {}; {} ".format(self._source.name, str(self._fs),str(self._path))


class SourceFile(object):
    """Base class for accessors that generate rows from a soruce file """

    def __init__(self, spec, fstor):
        """

        :param flo: A File-like object for the file, already opened.
        :return:
        """

        self.spec = spec
        self._fstor = fstor
        self.headers = []

    def coalesce_headers(self, header_lines):

        if len(header_lines) > 1:

            # If there are gaps in the values in the first header line, extend them forward
            hl1 = []
            last = None
            for x in header_lines[0]:
                if not x:
                    x = last
                else:
                    last = x

                hl1.append(x)

                header_lines[0] = hl1

            headers = [' '.join(col_val.strip() if col_val else '' for col_val in col_set)
                      for col_set in zip(*header_lines)]

            headers = [h.strip() for h in headers]

            return headers

        elif len(header_lines) > 0:
            return header_lines[0]

        else:
            return []

    def __iter__(self):

        rg = self._get_row_gen()

        header_lines = self.spec.header_lines
        start_line = self.spec.start_line
        end_line = self.spec.end_line

        headers = []
        comments = []

        # The loop is broken up into parts to remove as much of the logic as possible in the
        # majority of cases. It's an easy, although small, optimization

        self.start()

        # Wrap the rg to isolate it from the starting and stopping of the multiuple loops. The muti-loop
        # situation isn't a problem for most generators, but the ones created by PETL will restart.
        def wrap_rg():
            for row in rg:
                yield row

        wrapped_rg = wrap_rg()

        for i, row in enumerate(wrapped_rg):
            if header_lines and i in header_lines:
                headers.append(row)

            elif i == start_line:
                self.headers = self.coalesce_headers(headers)
                yield self.headers
                yield row
                break

            else:
                comments.append(row)

        if end_line:
            for i, row in enumerate(wrapped_rg):

                if i == end_line:
                    break

                yield row
        else:
            for i, row in enumerate(wrapped_rg):
                yield row

        self.finish()

    def _get_row_gen(self):
        pass

    def start(self):
        pass

    def finish(self):
        pass

class CsvSource(SourceFile):
    """Generate rows from a CSV source"""
    def _get_row_gen(self):
        import petl
        return petl.io.csv.fromcsv(self._fstor, self.spec.encoding)

class TsvSource(SourceFile):
    """Generate rows from a TSV ( Tab selerated value) source"""
    def _get_row_gen(self):
        import petl
        return petl.io.csv.fromtsv(self._fstor, self.spec.encoding)

class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def make_fw_row_parser(self):

        parts = []

        if not self.spec.columns:
            raise SourceError("Fixed width source much have a schema defined, with  column widths. ")

        for i, c in enumerate(self.spec.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise SourceError('Fixed width source {} must have start and width values for {} column '
                                .format(self.spec.name, c.name))

            parts.append('row[{}:{}]'  .format(c.start - 1, c.start + c.width - 1))

        return eval('lambda row: [{}]'.format(','.join(parts)))

    def headers(self):
        return [ c.name if c.name else i for i, c in enumerate(self.spec.columns) ]

    def _get_row_gen(self):

        flo = self._fstor.open(mode='r', encoding=self.spec.encoding)
        parser = self.make_fw_row_parser()

        yield self.headers

        for line in flo:
            yield [e.strip() for e in parser(line.strip())]

    def __iter__(self):
        rg = self._get_row_gen()
        self.start()
        for row in rg:
            yield row

        self.finish()


class PartitionSource(SourceFile):
    """Generate rows from an excel file"""
    def _get_row_gen(self):

        for row in self.bundle.library.partition(self.spec.url).stream():
            yield row


class ExcelSource(SourceFile):
    """Generate rows from an excel file"""
    def _get_row_gen(self):
        from fs.errors import  NoSysPathError

        try:
            return self.excel_iter(self._fstor.syspath(), self.spec.segment)
        except NoSysPathError:
            # There is no sys path when the file is in a ZipFile, or other non-traditional filesystem.
            from fs.opener import fsopendir
            from ambry.util.flo import copy_file_or_flo
            from os.path import dirname, join

            sub_file = self._fstor.sub_cache()

            with self._fstor.open(mode='rb') as f_in, sub_file.open(self.spec.name, mode='wb') as f_out:
                copy_file_or_flo(f_in, f_out)

            spath = sub_file.getsyspath(self.spec.name)

            return self.excel_iter(spath, self.spec.segment)

    def excel_iter(self, file_name, segment):
        from xlrd import open_workbook

        def srow_to_list(row_num, s):
            """Convert a sheet row to a list"""

            values = []

            for col in range(s.ncols):
                values.append(s.cell(row_num, col).value)

            return values

        wb = open_workbook(file_name)

        s = wb.sheets()[int(segment) if segment else 0]

        for i in range(0, s.nrows):
            row = srow_to_list(i, s)
            yield row

    @staticmethod
    def make_excel_date_caster(file_name):
        """Make a date caster function that can convert dates from a particular workbook. This is required
        because dates in Excel workbooks are stupid. """

        from xlrd import open_workbook

        wb = open_workbook(file_name)
        datemode = wb.datemode

        def excel_date(v):
            from xlrd import xldate_as_tuple
            import datetime

            try:

                year, month, day, hour, minute, second = xldate_as_tuple(float(v), datemode)
                return datetime.date(year, month, day)
            except ValueError:
                # Could be actually a string, not a float. Because Excel dates are completely broken.
                from dateutil import parser

                try:
                    return parser.parse(v).date()
                except ValueError:
                    return None

        return excel_date


class GoogleSource(SourceFile):
    """Generate rows from a CSV source

    To read a GoogleSpreadsheet, you'll need to have an account entry for google_spreadsheets, and the
    spreadsheet must be shared with the client email defined in the credentials.

    Visit http://gspread.readthedocs.org/en/latest/oauth2.html to learn how to generate the cerdential file, then
    copy the entire contents of the file into the a 'google_spreadsheets' key in the accounts file.

    Them share the google spreadsheet document with the email addressed defined in the 'client_email' entry of
    the credentials.

    """


    def _get_row_gen(self):

        """"Iterate over the rows of a goodl spreadsheet. The URL field of the source must start with gs:// followed by
        the spreadsheet key. """

        for row in self._fstor.get_all_values():
            yield row


class ColumnSpec(object):

    def __init__(self, name, position = None, start=None, width = None):
        """

        :param name:
        :param position:
        :param start:
        :param width:
        :return:
        """

        self.name = name
        self.position = position
        self.start = start
        self.width = width

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return "ColumnSpec(**{})".format(str(self.__dict__))


class SourceSpec(object):

    def __init__(self, url, segment = None,
                 header_lines = [0], start_line = None, end_line = None,
                 urltype = None, filetype = None,
                 encoding = None,
                 columns = None, name = None, **kwargs):
        """

        :param segment:
        :param header_lines: A list of lines that hold headers
        :param start_lines: The source line on which row data starts. Defaults to 1
        :param end_lines: The source line on which row data ends.
        :param urltype:
        :param filetype:
        :param encoding:
        :param columns: A list or tuple of column specs
        :param name: An optional name for the source
        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:
        """

        self.url = url
        self.name = name
        self.segment = segment
        self.header_lines = header_lines
        self.start_line = start_line
        self.end_line = end_line
        self.urltype = urltype
        self.filetype = filetype
        self.encoding = encoding
        self.columns = columns

        self.encoding = self.encoding if self.encoding else None

        if self.header_lines:
            if isinstance(self.header_lines, basestring):
                self.header_lines = self.header_lines.split(',')

            self.header_lines = [ int(e) for e in self.header_lines]

            if self.start_line is None:
                if len(self.header_lines) > 1:
                    self.start_line = max(*self.header_lines) + 1
                else:
                    self.start_line = self.header_lines[0] + 1

        elif self.header_lines is None or self.header_lines is False or self.header_lines.lower() == 'none':
            # None or False means that there is no header
            self.header_lines = None
            self.start_line = 0

        else:
            # Other non-true values mean that the header was not specified, so it defaults to the
            # first line
            self.header_lines = [0]
            self.start_line = 1

        if not self.name:
            import hashlib
            self.name = hashlib.md5(self.url+str(self.segment))


class RowProxy(object):
    '''
    A dict-like accessor for rows which holds a constant header for the keys. Allows for faster access than
    constructing a dict, and also provides attribute access

    >>> header = list('abcde')
    >>> rp = RowProxy(header)
    >>> for i in range(10):
    >>>     row = [ j for j in range(len(header)]
    >>>     rp.set_row(row)
    >>>     print rp['c']

    '''

    def __init__(self, keys):

        self.__keys = keys
        self.__row = [None] * len(keys)
        self.__pos_map = { e:i for i, e in enumerate(keys)}
        self.__initialized = True

    @property
    def row(self):
        return object.__getattribute__(self, '_RowProxy__row')

    def set_row(self,v):
        object.__setattr__(self, '_RowProxy__row', v)
        return self

    @property
    def headers(self):
        return self.__getattribute__('_RowProxy__keys')

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.__row[key] = value
        else:
            self.__row[self.__pos_map[key]] = value

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.__row[key]
        else:
            return self.__row[self.__pos_map[key]]

    def __setattr__(self, key, value):

        if not self.__dict__.has_key('_RowProxy__initialized'):
            return object.__setattr__(self, key, value)

        else:
            self.__row[self.__pos_map[key]] = value

    def __getattr__(self, key):

        return self.__row[self.__pos_map[key]]

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.__keys)

    def __len__(self):
        return len(self.__keys)

    @property
    def dict(self):
        return dict(zip(self.__keys, self.__row))

    # The final two methods aren't required, but nice for demo purposes:
    def __str__(self):
        '''returns simple dict representation of the mapping'''
        return str(self.dict)

    def __repr__(self):
        return self.dict.__repr__()