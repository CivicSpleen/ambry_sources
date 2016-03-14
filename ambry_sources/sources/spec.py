# -*- coding: utf-8 -*-
import hashlib

from six import string_types, text_type


class ColumnSpec(object):

    def __init__(self, name, position=None, start=None, width=None, **kwargs):
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
        return 'ColumnSpec({})'.format(','.join('{}={}'.format(k, v if not isinstance(v, string_types)
                                                else '"{}"'.format(v))
                                                for k, v in self.__dict__.items()))


class SourceSpec(object):

    def __init__(self, url, segment=None,
                 header_lines=False, start_line=None, end_line=None,
                 urltype=None, filetype=None,
                 encoding=None,
                 columns=None, name=None, file=None, **kwargs):
        """

        The ``header_lines`` can be a list of header lines, or one of a few special values:

        * [0]. The header line is the first line in the dataset.
        * False. The header line is not specified, so it should be intuited
        * None or 'none'. There is no header line, and it should not be intuited.

        :param segment:
        :param header_lines: A list of lines that hold headers
        :param start_lines: The source line on which row data starts. Defaults to 1
        :param end_lines: The source line on which row data ends.
        :param urltype:
        :param filetype:
        :param encoding:
        :param columns: A list or tuple of ColumnSpec objects, for FixedSource
        :param name: An optional name for the source
        :param kwargs: Unused. Provided to make it easy to load a record from a dict.
        :return:
        """

        if 'reftype' in kwargs and not urltype:
            urltype = kwargs['reftype']  # Ambry SourceFile object changed from urltype to reftype.

        def norm(v):

            if v == 0:
                return 0

            if bool(v):
                return v
            else:
                return None

        assert not isinstance(columns, dict)

        try:
            assert not isinstance(columns[0], dict)
        except:
            pass

        self.url = url
        self.name = name
        self.segment = segment
        self.header_lines = header_lines
        self.start_line = norm(start_line)
        self.end_line = norm(end_line)
        self.urltype = urltype
        self.filetype = filetype
        self.encoding = encoding
        self.columns = columns
        self.file = file

        self._header_lines_specified = False

        self.download_time = None  # Set externally

        self.encoding = self.encoding if self.encoding else None

        # If the header lines is specified as a comma delimited list
        if isinstance(self.header_lines, string_types) and self.header_lines != 'none':
            self.header_lines = [int(e) for e in self.header_lines.split(',') if e.strip() != '']

        # If it is an actual list.
        elif isinstance(self.header_lines, (list, tuple)):
            self.header_lines = [int(e) for e in self.header_lines if str(e).strip() != '']

        if self.header_lines:
            self.start_line = max(self.header_lines) + 1

        if not self.name:
            raw_name = '{}{}'.format(self.url, self.segment)
            if isinstance(raw_name, text_type):
                raw_name = raw_name.encode('utf-8')
            self.name = hashlib.md5(raw_name).hexdigest()

    @property
    def has_rowspec(self):
        """Return True if the spec defines header lines or the data start line"""
        return self._header_lines_specified or self.start_line is not None

    def get_filetype(self, file_path):
        """Determine the format of the source file, by reporting the file extension"""
        from os.path import splitext

        # The filetype is explicitly specified
        if self.filetype:
            return self.filetype.lower()

        root, ext = splitext(file_path)

        return ext[1:].lower()

    def get_urltype(self):
        from os.path import splitext

        if self.urltype:
            return self.urltype

        if self.url and self.url.startswith('gs://'):
            return 'gs'  # Google spreadsheet

        if self.url and self.url.startswith('file://'):
            return 'file'  # Google spreadsheet

        if self.url:

            if '#' in self.url:
                url, frag = self.url.split('#')
            else:
                url = self.url

            root, ext = splitext(url)
            return ext[1:].lower()

        return None

    def __str__(self):
        return str(self.__dict__)
