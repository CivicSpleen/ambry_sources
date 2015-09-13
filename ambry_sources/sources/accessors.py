"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""
import fiona
from shapely.geometry import shape
from shapely.wkt import dumps, loads

import petl

from ambry_sources.util import copy_file_or_flo

from .exceptions import SourceError


class SourceFile(object):
    """Base class for accessors that generate rows from a source file

    FIXME: must override _get_row_gen at least.
    """

    def __init__(self, spec, fstor, use_row_spec=True):
        """

        :param fstor: A File-like object for the file, already opened.
        :return:
        """

        self.spec = spec
        self._fstor = fstor
        self.headers = []
        self.use_row_spec = use_row_spec

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

        self.start()

        if self.use_row_spec:
            header_lines = self.spec.header_lines
            start_line = self.spec.start_line
            end_line = self.spec.end_line

            headers = []
            comments = []

            # The loop is broken up into parts to remove as much of the logic as possible in the
            # majority of cases. It's an easy, although small, optimization

            # Wrap the rg to isolate it from the starting and stopping of the multiple loops. The muti-loop
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

        else:

            for row in rg:
                yield row

        self.finish()

    def raw_iter(self):

        self.start()

        for row in self._get_row_gen():
            yield row

        self.finish()

    def _get_row_gen(self):
        """ Returns generator over all rows of the source. """
        # FIXME:
        # raise NotImplementedError
        pass

    def start(self):
        pass

    def finish(self):
        pass


class GeneratorSource(SourceFile):
    def __init__(self, spec, generator, use_row_spec=True):
        super(GeneratorSource, self).__init__(spec, None, use_row_spec)
        self.gen = generator

    def _get_row_gen(self):
        return self.gen


class CsvSource(SourceFile):
    """Generate rows from a CSV source"""
    def _get_row_gen(self):
        return petl.io.csv.fromcsv(self._fstor, self.spec.encoding)


class TsvSource(SourceFile):
    """Generate rows from a TSV (tab separated value) source"""
    def _get_row_gen(self):
        return petl.io.csv.fromtsv(self._fstor, self.spec.encoding)


class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def make_fw_row_parser(self):

        parts = []

        if not self.spec.columns:
            raise SourceError('Fixed width source much have a schema defined, with  column widths.')

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
        return [c.name if c.name else i for i, c in enumerate(self.spec.columns)]

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
        from fs.errors import NoSysPathError

        try:
            return self.excel_iter(self._fstor.syspath(), self.spec.segment)
        except NoSysPathError:
            # There is no sys path when the file is in a ZipFile, or other non-traditional filesystem.
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

            try:
                for col in range(s.ncols):
                    values.append(s.cell(row_num, col).value)
            except:
                print '!!!!', row_num
                raise

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
        """Iterate over the rows of a google spreadsheet.

        Note:
            The URL field of the source must start with gs:// followed by the spreadsheet key.
        """

        for row in self._fstor.get_all_values():
            yield row


class GeoSourceBase(SourceFile):
    """Base class for all geo sources."""
    pass


class ShapefileSource(GeoSourceBase):

    def _get_row_gen(self):
        """ Returns generator over shapefile rows.

        Note:
            The first column is an id field, taken from the id value of each shape
            The middle values are taken from the property_schema
            The last column is a string named geometry, which has the wkt value, the type is geometry_type.

        """

        with fiona.drivers():
            virtual_fs = self._fstor.system_path
            layer_index = self.spec.segment or 0
            with fiona.open('/', vfs=virtual_fs, layer=layer_index) as source:
                geometry_type = source.schema['geometry']
                property_schema = source.schema['properties']

                for s in source:
                    row_data = s['properties']
                    shp = shape(s['geometry'])
                    wkt = dumps(shp)
                    row = ['idFIXME:']
                    for col_name, elem in row_data.iteritems():
                        row.append(elem)
                    row.append(wkt)
                    yield row
