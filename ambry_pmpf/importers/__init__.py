"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import logging

from .. import SourceFile

class SourceError(Exception):
    pass




class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
    def __init__(self, source, fs, path, mode='r', from_cache=False, account_accessor=None):
        self._source = source
        self._fs = fs
        self._path = path
        self._mode = mode
        self._account_accessor = account_accessor

        self.from_cache = from_cache

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    def syspath(self):
        return self._fs.getsyspath(self._path)

    def source_pipe(self):
        return self._source.row_gen()

    @property
    def path(self):
        return self._path

    def __str__(self):

        from fs.errors import NoSysPathError

        try:
            return self.syspath()
        except NoSysPathError:
            return "Delayed Open: source = {}; {}; {} ".format(self._source.name, str(self._fs),str(self._path))


class CsvSource(SourceFile):
    """Generate rows from a CSV source"""
    def _get_row_gen(self):
        import petl
        fstor = self.fetch()
        return petl.io.csv.fromcsv(fstor, self._source.encoding if self._source.encoding else None)


class TsvSource(SourceFile):
    """Generate rows from a TSV ( Tab selerated value) source"""
    def _get_row_gen(self):
        import petl

        fstor = self.fetch()
        return petl.io.csv.fromtsv(fstor, self._source.encoding if self._source.encoding else None)


class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def fixed_width_iter(self, flo, source):

        parts = []
        self.headers = []  # THe header will be the column positions.
        for i, c in enumerate(source.source_table.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise TypeError('Source table {} must have start and width values for {} column '
                                .format(source.source_table.name, c.source_header))

            parts.append('row[{}:{}]'.format(c.start - 1, c.start + c.width - 1))
            self.headers.append('{}:{}'.format(c.start - 1, c.start + c.width - 1))

        parser = eval('lambda row: [{}]'.format(','.join(parts)))

        yield source.source_table.headers

        for line in flo:
            yield [e.strip() for e in parser(line.strip())]

    def _get_row_gen(self):

        fstor = self.fetch()
        return self.fixed_width_iter(fstor.open(mode='r', encoding=self._source.encoding), self._source)

    def __iter__(self):
        rg = self._get_row_gen()
        self.start()
        for row in rg:
            yield row

        self.finish()


class PartitionSource(SourceFile):
    """Generate rows from an excel file"""
    def _get_row_gen(self):

        for row in self.bundle.library.partition(self.source.url).stream():
            yield row


class ExcelSource(SourceFile):
    """Generate rows from an excel file"""
    def _get_row_gen(self):
        from fs.errors import  NoSysPathError
        fstor = self.fetch()
        try:
            return self.excel_iter(fstor.syspath(), self._source.segment)
        except NoSysPathError:
            # There is no sys path when the file is in a ZipFile, or other non-traditional filesystem.
            from fs.opener import fsopendir
            from ambry.util.flo import copy_file_or_flo
            from os.path import dirname, join

            cache = self.bundle.library.download_cache
            path = join(self.bundle.identity.cache_key, self._source.name)
            cache.makedir(dirname(path), recursive = True, allow_recreate=True) #FIXME: Should check that file exists

            with fstor.open(mode='rb') as f_in, cache.open(path, 'wb') as f_out:
                copy_file_or_flo(f_in, f_out)

            return self.excel_iter(cache.getsyspath(path), self._source.segment)

    def excel_iter(file_name, segment):
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

        for row in wksht.get_all_values():
            yield row


