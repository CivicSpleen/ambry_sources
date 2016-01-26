"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import petl

import six

from ambry_sources.util import copy_file_or_flo

from .exceptions import SourceError


class Source(object):
    """Base class for accessors that generate rows from any source

    Subclasses of Source must override at least _get_row_gen method.
    """

    def __init__(self, spec):
        from copy import deepcopy

        try:
            self.spec = deepcopy(spec)
        except TypeError:
            pass

    @property
    def headers(self):
        """Return a list of the names of the columns of this file, or None if the header is not defined.

        This should *only* return headers if the headers are unambiguous, such as for database tables,
        or shapefiles. For other files, like CSV and Excel, the header row can not be determined without analysis
        or specification."""

        return None

    @headers.setter
    def headers(self, v):
        raise NotImplementedError

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for row in self._get_row_gen():
            yield row

        self.finish()

    def _get_row_gen(self):
        """ Returns generator over all rows of the source. """
        raise NotImplementedError('Subclasses of SourceFile must provide a _get_row_gen() method')

    def start(self):
        pass

    def finish(self):
        pass


class SourceFile(Source):
    """Base class for accessors that generate rows from a source file

    Subclasses of SourceFile must override at lease _get_row_gen method.
    """

    def __init__(self, spec, fstor):
        """

        :param fstor: A File-like object for the file, already opened.
        :return:
        """
        super(SourceFile, self).__init__(spec)

        self._fstor = fstor
        self._headers = None  # Reserved for subclasses that extract headers from data stream

    @property
    def path(self):
        return self._fstor.path

    @property
    def syspath(self):
        return self._fstor.syspath

    def coalesce_headers(self, header_lines):
        """Collect multiple header lines from the preamble and assemble them into a single header line"""

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


class GeneratorSource(Source):

    def __init__(self, spec, generator):
        super(GeneratorSource, self).__init__(spec)

        if not (not spec.start_line or spec.start_line == 1):
            raise SourceError("For GeneratorSource, the start line must be 1 or unspecified; got '{}' "
                              .format(spec.start_line))

        if not (not spec.header_lines or spec.header_lines == [0] or spec.header_lines == []):
            raise SourceError("For GeneratorSource, the start line must be [0] or unspecified; got '{}'"
                              .format(spec.header_lines))

        self.spec.start_line = 1
        self.spec.header_lines = [0]

        self.gen = generator

        if six.callable(self.gen):
            self.gen = self.gen()

    def __iter__(self):
        """ Iterate over all of the lines in the generator. """
        # TODO (kazbek): Isn't returning self.gen from _get_row_gen method a better choice? Try it.

        self.start()

        for row in self.gen:
            yield row

        self.finish()


class DatabaseRelationSource(Source):
    """ Source for database table or view. """

    def __init__(self, spec, engine_name, connection):
        """
        Args:
            spec (ambry_sources.sources.spec.SourceSpec):
            connection (sqlalchemy.engine.Connection):
        """
        super(DatabaseRelationSource, self).__init__(spec)
        self._connection = connection
        self._engine_name = engine_name

    @property
    def headers(self):
        return [x['name'] for x in self._get_columns()]

    def _get_columns(self):
        ret = []
        if self._engine_name == 'sqlite':
            result = self._connection.execute('PRAGMA table_info(\'{}\');'.format(self.spec.url))

            for row in result:
                position = row[0]
                name = row[1]
                ret.append({
                    'name': name,
                    'position': position
                })
        elif self._engine_name == 'postgresql':
            query = '''
                SELECT attr.attname, attr.attnum
                FROM pg_attribute AS attr
                JOIN pg_class AS cls ON cls.oid = attr.attrelid
                JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                WHERE attr.attnum > 0
                    AND cls.relkind in ('r', 'v', 'm')
                    AND cls.relname = '{table}'
                    AND ns.nspname = '{schema}'
                    AND NOT attr.attisdropped
                ORDER BY attr.attnum;
            '''
            result = self._connection.execute(query.format(schema='ambrylib', table=self.spec.url))
            for row in result:
                name = row[0]
                position = row[1]
                ret.append({
                    'name': row[0],
                    'position': row[1]
                })
        return ret

    def _get_row_gen(self):
        if self._engine_name == 'postgresql':
            return self._connection.execute('SELECT * FROM {}.{};'.format('ambrylib', self.spec.url))
        else:
            return self._connection.execute('SELECT * FROM {};'.format(self.spec.url))


class MPRSource(Source):

    def __init__(self, spec, datafile, predicate=None, headers=None):
        super(MPRSource, self).__init__(spec)

        if not (not spec.start_line or spec.start_line == 1):
            raise SourceError("For MPRSource, the start line must be 1 or unspecified; got '{}' "
                              .format(spec.start_line))

        if not (not spec.header_lines or spec.header_lines == [0] or spec.header_lines == []):
            raise SourceError("For MPRSource, the start line must be [0] or unspecified; got '{}'"
                              .format(spec.header_lines))

        self.datafile = datafile

        self.spec.start_line = 1
        self.spec.header_lines = [0]

        self.predicate = predicate
        self.return_headers = headers

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        with self.datafile.reader as r:
            for i, row in enumerate(r.select(predicate=self.predicate, headers=self.return_headers)):

                if i == 0:
                    yield row.headers

                yield row.row  # select returns a RowProxy

        self.finish()


class CsvSource(SourceFile):
    """Generate rows from a CSV source"""

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        import sys


        self.start()

        encoding = self.spec.encoding or 'utf8'

        try:

            if sys.version_info[0] >= 3:  # Python 3
                import csv
                f = open(self._fstor.syspath, 'rtU', encoding=encoding)
                reader = csv.reader(f)
            else:  # Python 2
                import unicodecsv as csv
                f = open(self._fstor.syspath, 'rbU')
                reader = csv.reader(f, encoding=encoding)

            i = 0
            try:
                for row in reader:
                    i += 1
                    yield row
            except Exception as e:
                raise
                from ambry_sources.sources.exceptions import SourceError
                raise SourceError(str(type(e)) + ';' + e.message + "; line={}".format(i))


        finally:
            f.close()

        self.finish()


class TsvSource(SourceFile):
    """Generate rows from a TSV (tab separated value) source"""

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for i, row in enumerate(petl.io.csv.fromtsv(self._fstor, self.spec.encoding)):
            yield row

        self.finish()


class FixedSource(SourceFile):
    """Generate rows from a fixed-width source"""

    def __init__(self, spec, fstor):
        from .exceptions import SourceError

        super(FixedSource, self).__init__(spec, fstor)

        if not (spec.start_line is None or spec.start_line == 1):
            raise SourceError("For FixedSource, the start line must be 1 or unspecified; got '{}' "
                              .format(spec.start_line))

        if not (not spec.header_lines or spec.header_lines == [0] or spec.header_lines == []):
            raise SourceError("For FixedSource, the header_lines must be [0] or unspecified; got '{}'"
                              .format(spec.header_lines))

    def make_fw_row_parser(self):

        parts = []

        if not self.spec.columns:
            raise SourceError('Fixed width source must have a schema defined, with column widths.')

        for i, c in enumerate(self.spec.columns):

            try:
                int(c.start)
                int(c.width)
            except TypeError:
                raise SourceError('Fixed width source {} must have start and width values for {} column '
                                  .format(self.spec.name, c.name))

            parts.append('row[{}:{}]'  .format(c.start - 1, c.start + c.width - 1))

        code = 'lambda row: [{}]'.format(','.join(parts))

        return eval(code)

    @property
    def headers(self):

        return [c.name if c.name else i for i, c in enumerate(self.spec.columns)]

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        parser = self.make_fw_row_parser()

        for line in self._fstor.open(mode='r', encoding=self.spec.encoding):

            yield [e.strip() for e in parser(line)]

        self.finish()


class PartitionSource(SourceFile):
    """Generate rows from a partition. """

    def _get_row_gen(self):
        # TODO: Where is self.bundle definition?
        for row in self.bundle.library.partition(self.spec.url).stream():
            yield row


class ExcelSource(SourceFile):
    """Generate rows from an excel file"""

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for i, row in enumerate(self._get_row_gen()):

            if i == 0:
                self._headers = row

            yield row

        self.finish()

    def _get_row_gen(self):
        from fs.errors import NoSysPathError

        try:
            return self.excel_iter(self._fstor.syspath, self.spec.segment)
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
                print('!!!!', row_num)
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
    """Generate rows from a Google spreadsheet source

    To read a GoogleSpreadsheet, you'll need to have an account entry for google_spreadsheets, and the
    spreadsheet must be shared with the client email defined in the credentials.

    Visit http://gspread.readthedocs.org/en/latest/oauth2.html to learn how to generate the credential file, then
    copy the entire contents of the file into the a 'google_spreadsheets' key in the accounts file.

    Them share the google spreadsheet document with the email addressed defined in the 'client_email' entry of
    the credentials.

    """

    def __iter__(self):
        """Iterate over all of the lines in the file"""

        self.start()

        for row in self._fstor.get_all_values():
            yield row

        self.finish()


class GeoSourceBase(SourceFile):
    """ Base class for all geo sources. """
    pass


class ShapefileSource(GeoSourceBase):
    """ Accessor for shapefiles (*.shp) with geo data. """

    def __init__(self, spec, fstor):
        super(ShapefileSource, self).__init__(spec, fstor)

        if not (not spec.start_line or spec.start_line == 1):
            raise SourceError("For ShapefileSource, the start line must be 1 or unspecified; got '{}' "
                              .format(spec.start_line))

        if not (not spec.header_lines or spec.header_lines == [0] or spec.header_lines == []):
            raise SourceError("For ShapefileSource, the start line must be [0] or unspecified; got '{}'"
                              .format(spec.header_lines))

        self.spec.start_line = 0
        self.spec.header_lines = None

    def _convert_column(self, shapefile_column):
        """ Converts column from a *.shp file to the column expected by ambry_sources.

        Args:
            shapefile_column (tuple): first element is name, second is type.

        Returns:
            dict: column spec as ambry_sources expects

        Example:
            self._convert_column((u'POSTID', 'str:20')) -> {'name': u'POSTID', 'type': 'str'}

        """
        name, type_ = shapefile_column
        type_ = type_.split(':')[0]
        return {'name': name, 'type': type_}

    def _get_columns(self, shapefile_columns):
        """ Returns columns for the file accessed by accessor.

        Args:
            shapefile_columns (SortedDict): key is column name, value is column type.

        Returns:
            list: list of columns in ambry_sources format

        Example:
            self._get_columns(SortedDict((u'POSTID', 'str:20'))) -> [{'name': u'POSTID', 'type': 'str'}]

        """
        #
        # first column is id and will contain id of the shape.
        columns = [{'name': 'id', 'type': 'int'}]

        # extend with *.shp file columns converted to ambry_sources format.
        columns.extend(list(map(self._convert_column, iter(shapefile_columns.items()))))

        # last column is wkt value.
        columns.append({'name': 'geometry', 'type': 'geometry_type'})
        return columns

    def _get_row_gen(self):
        return iter(self)

    @property
    def headers(self):
        """Return headers. This must be run after iteration, since the value that is returned is
        set in iteration """
        return self._headers

    def __iter__(self):
        """ Returns generator over shapefile rows.

        Note:
            The first column is an id field, taken from the id value of each shape
            The middle values are taken from the property_schema
            The last column is a string named geometry, which has the wkt value, the type is geometry_type.

        """

        # These imports are nere, not at the module level, so the geo
        # support can be an extra

        import fiona

        from shapely.geometry import shape
        from shapely.wkt import dumps
        from .spec import ColumnSpec

        self.start()

        with fiona.drivers():
            # retrive full path of the zip and convert it to url
            virtual_fs = 'zip://{}'.format(self._fstor._fs.zf.filename)
            layer_index = self.spec.segment or 0
            with fiona.open('/', vfs=virtual_fs, layer=layer_index) as source:
                # geometry_type = source.schema['geometry']
                property_schema = source.schema['properties']
                self.spec.columns = [ColumnSpec(**c) for c in self._get_columns(property_schema)]
                self._headers = [x['name'] for x in self._get_columns(property_schema)]

                for s in source:
                    row_data = s['properties']
                    shp = shape(s['geometry'])
                    wkt = dumps(shp)
                    row = [int(s['id'])]
                    for col_name, elem in six.iteritems(row_data):
                        row.append(elem)
                    row.append(wkt)
                    yield row

        self.finish()
