
from collections import deque, OrderedDict
import datetime
import logging
import re

from six import string_types, iteritems, binary_type, text_type, b

logger = logging.getLogger(__name__)


class NoMatchError(Exception):
    pass


class unknown(binary_type):

    __name__ = 'unknown'

    def __new__(cls):
        return super(unknown, cls).__new__(cls, cls.__name__)

    def __str__(self):
        return self.__name__

    def __eq__(self, other):
        return binary_type(self) == binary_type(other)


def test_float(v):
    # Fixed-width integer codes are actually strings.
    # if v and v[0]  == '0' and len(v) > 1:
    # return 0

    try:
        float(v)
        return 1
    except:
        return 0


def test_int(v):
    # Fixed-width integer codes are actually strings.
    # if v and v[0] == '0' and len(v) > 1:
    # return 0

    try:
        if float(v) == int(float(v)):
            return 1
        else:
            return 0
    except:
        return 0


def test_string(v):
    if isinstance(v, string_types):
        return 1
    if isinstance(v, binary_type):
        return 1
    else:
        return 0


def test_datetime(v):
    """Test for ISO datetime."""
    if not isinstance(v, string_types):
        return 0

    if len(v) > 22:
        # Not exactly correct; ISO8601 allows fractional seconds
        # which could result in a longer string.
        return 0

    if '-' not in v and ':' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in 'T:-Z':
            return 0

    return 1


def test_time(v):
    if not isinstance(v, string_types):
        return 0

    if len(v) > 15:
        return 0

    if ':' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in 'T:Z.':
            return 0

    return 1


def test_date(v):
    if not isinstance(v, string_types):
        return 0

    if len(v) > 10:
        # Not exactly correct; ISO8601 allows fractional seconds
        # which could result in a longer string.
        return 0

    if '-' not in v:
        return 0

    for c in set(v):  # Set of Unique characters
        if not c.isdigit() and c not in '-':
            return 0

    return 1


tests = [
    (int, test_int),
    (float, test_float),
    (binary_type, test_string),
]


class Column(object):
    position = None
    header = None
    type_counts = None
    type_ratios = None
    length = 0
    count = 0
    strings = None

    def __init__(self):
        self.type_counts = {k: 0 for k, v in tests}
        self.type_counts[datetime.datetime] = 0
        self.type_counts[datetime.date] = 0
        self.type_counts[datetime.time] = 0
        self.type_counts[None] = 0
        self.type_counts[text_type] = 0
        self.strings = deque(maxlen=1000)
        self.position = None
        self.header = None
        self.count = 0
        self.length = 0
        self.date_successes = 0
        self.description = None

    def inc_type_count(self, t):
        self.type_counts[t] += 1

    def test(self, v):
        from dateutil import parser

        self.count += 1

        if v is None:
            self.type_counts[None] += 1
            return None

        try:
            v = '{}'.format(v).encode('ascii')
        except UnicodeEncodeError:
            self.type_counts[text_type] += 1
            return text_type

        self.length = max(self.length, len(v))

        try:
            v = v.strip()
        except AttributeError:
            pass

        if v == '':
            self.type_counts[None] += 1
            return None

        for test, testf in tests:
            t = testf(v)

            if t > 0:
                type_ = test

                if test == binary_type:
                    if v not in self.strings:
                        self.strings.append(v)

                    if (self.count < 1000 or self.date_successes != 0) and any((c in b('-/:T')) for c in v):
                        try:
                            maybe_dt = parser.parse(
                                v, default=datetime.datetime.fromtimestamp(0))
                        except (TypeError, ValueError):
                            maybe_dt = None

                        if maybe_dt:
                            # Check which parts of the default the parser didn't change to find
                            # the real type
                            # HACK The time check will be wrong for the time of
                            # the start of the epoch, 16:00.
                            if maybe_dt.time() == datetime.datetime.fromtimestamp(0).time():
                                type_ = datetime.date
                            elif maybe_dt.date() == datetime.datetime.fromtimestamp(0).date():
                                type_ = datetime.time
                            else:
                                type_ = datetime.datetime

                            self.date_successes += 1

                self.type_counts[type_] += 1

                return type_

    def _resolved_type(self):
        """Return the type for the columns, and a flag to indicate that the
        column has codes."""
        import datetime

        self.type_ratios = {test: (float(self.type_counts[test]) / float(self.count)) if self.count else None
                            for test, testf in tests + [(None, None)]}

        # If it is more than 5% str, it's a str
        if self.type_ratios[binary_type] > .05:
            return binary_type, False

        if self.type_counts[datetime.datetime] > 0:
            num_type = datetime.datetime

        elif self.type_counts[datetime.date] > 0:
            num_type = datetime.date

        elif self.type_counts[datetime.time] > 0:
            num_type = datetime.time

        elif self.type_counts[float] > 0:
            num_type = float

        elif self.type_counts[int] > 0:
            num_type = int

        elif self.type_counts[binary_type] > 0:
            num_type = binary_type

        elif self.type_counts[text_type] > 0:
            num_type = text_type

        else:
            num_type = unknown

        if self.type_counts[binary_type] > 0 and num_type != binary_type:
            has_codes = True
        else:
            has_codes = False

        return num_type, has_codes

    @property
    def resolved_type(self):
        return self._resolved_type()[0]

    @property
    def resolved_type_name(self):
        try:
            return self.resolved_type.__name__
        except AttributeError:
            return self.resolved_type

    @property
    def has_codes(self):
        return self._resolved_type()[1]


class TypeIntuiter(object):
    """Determine the types of rows in a table."""
    header = None
    counts = None

    def __init__(self):
        self._columns = OrderedDict()

    def process_header(self, row):

        header = row # Huh? Don't remember what this is for.
        for i, value in enumerate(row):
            if i not in header:
                self._columns[i] = Column()
                self._columns[i].position = i
                self._columns[i].header = value

        return self

    def process_row(self, n, row):

        for i, value in enumerate(row):
            try:
                if i not in self._columns:
                    self._columns[i] = Column()
                    self._columns[i].position = i
                self._columns[i].test(value)

            except Exception as e:
                # This usually doesn't matter, since there are usually plenty of other rows to intuit from
                # print 'Failed to add row: {}: {} {}'.format(row, type(e), e)
                print(i, value, e)
                raise

    def run(self, source, total_rows=None):

        MIN_SKIP_ROWS = 10000

        if total_rows and total_rows > MIN_SKIP_ROWS:
            skip_rows = int(total_rows / MIN_SKIP_ROWS)

            skip_rows = skip_rows if skip_rows > 1 else None

        else:
            skip_rows = None

        for i, row in enumerate(iter(source)):
            if skip_rows and i % skip_rows != 0:
                continue
            self.process_row(i, row)

        return self

    @property
    def columns(self):

        for k, v in iteritems(self._columns):
            v.position = k
            yield v

    def __str__(self):
        from tabulate import tabulate

        # return  SingleTable([[ str(x) for x in row] for row in self.rows] ).table

        results = self.results_table()

        if len(results) > 1:
            o = '\n' + binary_type(tabulate(results[1:], results[0], tablefmt='pipe'))
        else:
            o = ''

        return 'TypeIntuiter ' + o

    @staticmethod
    def normalize_type(typ):

        if isinstance(typ, string_types):
            import datetime

            m = dict(list(__builtins__.items()) + list(datetime.__dict__.items()))
            if typ == 'unknown':
                typ = binary_type
            else:
                typ = m[typ]

        return typ

    @staticmethod
    def promote_type(orig_type, new_type):
        """Given a table with an original type, decide whether a new determination of a new applicable type
        should overide the existing one"""

        if not new_type:
            return orig_type

        if not orig_type:
            return new_type

        try:
            orig_type = orig_type.__name__
        except AttributeError:
            pass

        try:
            new_type = new_type.__name__
        except AttributeError:
            pass

        type_precidence = ['unknown', 'int', 'float', 'date', 'time', 'datetime', 'str', 'bytes', 'unicode']

        # TODO This will fail for dates and times.

        if type_precidence.index(new_type) > type_precidence.index(orig_type):
            return new_type
        else:
            return orig_type

    def results_table(self):

        fields = 'position header length resolved_type has_codes count ints floats strs unicode nones datetimes dates times '.split()

        header = list(fields)
        # Shorten a few of the header names
        header[0] = '#'
        header[2] = 'size'
        header[4] = 'codes'
        header[9] = 'uni'
        header[11] = 'dt'

        rows = list()

        rows.append(header)

        for d in self._dump():
            rows.append([d[k] for k in fields])

        return rows

    def _dump(self):

        for v in self.columns:
            d = {
                'position': v.position,
                'header': v.header,
                'length': v.length,
                'resolved_type': v.resolved_type_name,
                'has_codes': v.has_codes,
                'count': v.count,
                'ints': v.type_counts.get(int, None),
                'floats': v.type_counts.get(float, None),
                'strs': v.type_counts.get(binary_type, None),
                'unicode': v.type_counts.get(text_type, None),
                'nones': v.type_counts.get(None, None),
                'datetimes': v.type_counts.get(datetime.datetime, None),
                'dates': v.type_counts.get(datetime.date, None),
                'times': v.type_counts.get(datetime.time, None),
                'strvals': b(',').join(list(v.strings)[:20])
            }
            yield d


class ClusterHeaders(object):
    """Using Source table headers, cluster the source tables into destination tables"""

    def __init__(self, bundle=None):
        self._bundle = bundle
        self._headers = {}

    def match_headers(self, a, b):
        from difflib import ndiff
        from collections import Counter

        c = Counter(e[0] for e in ndiff(a, b) if e[0] != '?')

        same = c.get(' ', 0)
        remove = c.get('-', 0)
        add = c.get('+', 0)

        return float(remove+add) / float(same)

    def match_headers_a(self, a, b):
        from difflib import SequenceMatcher

        for i, ca in enumerate(a):
            for j, cb in enumerate(b):
                r = SequenceMatcher(None, ca, cb).ratio()

                if r > .9:
                    print(ca, cb)
                    break

    def add_header(self, name, headers):
        self._headers[name] = headers

    def pairs(self):
        return set([(name1, name2) for name1 in list(self._headers) for name2 in list(self._headers) if name2 > name1])

    @classmethod
    def long_substr(cls, data):
        data = list(data)
        substr = ''
        if len(data) > 1 and len(data[0]) > 0:
            for i in range(len(data[0])):
                for j in range(len(data[0]) - i + 1):
                    if j > len(substr) and cls.is_substr(data[0][i:i + j], data):
                        substr = data[0][i:i + j]
        return substr

    @classmethod
    def is_substr(cls, find, data):
        if len(data) < 1 and len(find) < 1:
            return False
        for i in range(len(data)):
            if find not in data[i]:
                return False
        return True

    def cluster(self):

        pairs = self.pairs()

        results = []
        for a, b_ in pairs:
            results.append((round(self.match_headers(self._headers[a], self._headers[b_]), 3), a, b_))

        results = sorted(results, key=lambda r: r[0])

        clusters = []

        for r in results:
            if r[0] < .3:
                a = r[1]
                b = r[2]
                allocated = False
                for c in clusters:
                    if a in c or b in c:
                        c.add(a)
                        c.add(b)
                        allocated = True
                        break
                if not allocated:
                    ns = set([a, b])
                    clusters.append(ns)

        d = {self.long_substr(c).strip('_'): sorted(c) for c in clusters}

        return d


class RowIntuiter(object):

    N_TEST_ROWS = 150

    type_map = {
        text_type: binary_type,
        float: int}

    def __init__(self):
        self.comment_lines = []
        self.header_lines = []
        self.start_line = 0
        self.end_line = 0

        self.data_pattern_source = None

        self.patterns = (
            ('B', re.compile(r'^_+$')),  # Blank
            ('C', re.compile(r'^XX_+$')),  # Comment
            ('C', re.compile(r'^X_+$')),  # Comment
            ('H', re.compile(r'^X+$')),  # Header
            ('H', re.compile(r'^_{,6}X+$')),  # Header, A few starting blanks, the rest are strings.
            ('H', re.compile(r"(?:X_)")),  # Header
        )

        self.test_rows = []

        self.debug = False

    def picture(self, row):
        """Create a simplified character representation of the data row, which can be pattern matched
        with a regex """

        template = '_Xn'
        types = (type(None), binary_type, int)

        def guess_type(v):

            v = text_type(v).strip()

            if not bool(v):
                return type(None)

            for t in (float, int, binary_type, text_type):
                try:
                    return type(t(v))
                except:
                    pass

        def p(e):
            try:
                t = guess_type(e)
                tm = self.type_map.get(t, t)
                return template[types.index(tm)]
            except ValueError:
                raise ValueError("Type '{}'/'{}' not in the types list: {}".format(t, tm, types))

        return ''.join(p(e) for e in row)

    def _data_pattern_source(self, rows, change_limit=5):

        l = max(len(row) for row in rows)  # Length of longest row

        patterns = [set() for _ in range(l)]

        contributors = 0  # Number  of rows that contributed to pattern.

        for j, row in enumerate(rows):

            changes = sum(1 for i, c in enumerate(self.picture(row)) if c not in patterns[i])

            # The pattern should stabilize quickly, with new rows not changing many cells. If there is
            # a large change, ignore it, as it may be spurious
            if j > 0 and changes > change_limit:
                continue

            contributors += 1

            for i, c in enumerate(self.picture(row)):
                patterns[i].add(c)

        pattern_source = ''.join("(?:{})".format('|'.join(s)) for s in patterns)

        return pattern_source, contributors, l

    def data_pattern(self, rows):

        tests = 50
        test_rows = min(20, len(rows))
        n_cols = None

        def try_tests(tests, test_rows, rows):
            # Look for the first row where you can generate a data pattern that does not have a large number of changes
            # in subsequent rows.
            for i in range(tests):

                max_changes = len(rows[0])/4 # Data row should have fewer than 25% changes compared to next

                test_rows_slice = rows[i: i + test_rows]

                if not test_rows_slice:
                    continue

                pattern_source, contributors, l = self._data_pattern_source(test_rows_slice, max_changes)

                ave_cols = sum( 1 for r in test_rows_slice for c in r ) / len(test_rows_slice)

                # If more the 75% of the rows contributed to the pattern, consider it good
                if contributors > test_rows*.75:
                    return pattern_source, ave_cols


        pattern_source, ave_cols= try_tests(tests, test_rows, rows)

        if not pattern_source:
            from .exceptions import RowIntuitError
            raise RowIntuitError('Failed to find data pattern')

        pattern = re.compile(pattern_source)

        return pattern, pattern_source, ave_cols

    @staticmethod
    def match_picture(picture, patterns):
        for l, r in patterns:
            if r.search(picture):
                return l

        return False

    def run(self, head_rows, tail_rows=None, n_rows=None):

        header_rows = []
        found_header = False

        data_pattern_skip_rows = min(30, len(head_rows) - 8)

        data_pattern, self.data_pattern_source, n_cols = self.data_pattern(head_rows[data_pattern_skip_rows:])

        patterns = ([('D', data_pattern),
                     # More than 25% strings in row is header, if it isn't matched as data
                     ('H', re.compile(r'X{{,{}}}'.format(n_cols/4))),
                     ] +
                    list(self.patterns))

        for i, row in enumerate(head_rows):

            picture = self.picture(row)

            label = self.match_picture(picture, patterns)

            try:
                # If a header or data has more than half of the line is a continuous nulls,
                # it's probably a comment.
                if label != 'B' and len(re.search('_+', picture).group(0)) > len(row)/2:
                    label = 'C'
            except AttributeError:
                pass  # re not matched

            if not found_header and label == 'H':
                found_header = True

            if label is False:

                if found_header:
                    label = 'D'
                else:
                    # Could be a really wacky header
                    found_header = True
                    label = 'H'

            if self.debug:
                print(i, label, picture, row)

            if label == 'C':
                self.comment_lines.append(i)

            elif label == 'H':
                self.header_lines.append(i)
                header_rows.append(row)

            elif label == 'D':
                self.start_line = i
                self.headers = self.coalesce_headers(header_rows)
                break

        if tail_rows:
            from itertools import takewhile

            # Compute the data label for the end line, then reverse them.
            labels = reversed(list(self.match_picture(self.picture(row), patterns) for row in tail_rows))

            # Count the number of lines, from the end, that are either comment or blank
            end_line = len(list(takewhile(lambda x: x == 'C' or x == 'B', labels)))

            if end_line:
                self.end_line = n_rows-end_line-1

        return self

    @classmethod
    def coalesce_headers(cls, header_lines):
        import re
        import six

        header_lines = [list(hl) for hl in header_lines if bool(hl)]

        if len(header_lines) == 0:
            return []

        if len(header_lines) == 1:
            return header_lines[0]

        # If there are gaps in the values of a line, copy them forward, so there
        # is some value in every position
        for hl in header_lines:
            last = None
            for i in range(len(hl)):
                hli = six.text_type(hl[i])
                if not hli.strip():
                    hl[i] = last
                else:
                    last = hli

        headers = [' '.join(text_type(col_val).strip() if col_val else '' for col_val in col_set)
                   for col_set in zip(*header_lines)]

        headers = [re.sub(r'\s+', ' ', h.strip()) for h in headers]

        return headers
