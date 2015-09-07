"""Intuit data types for rows of values."""
from __future__ import unicode_literals

from collections import deque
import datetime
import logging

from six import string_types, iteritems

logger = logging.getLogger(__name__)


class NoMatchError(Exception):
    pass


class unknown(str):

    __name__ = 'unknown'

    def __new__(cls):
        return super(unknown, cls).__new__(cls, cls.__name__)

    def __str__(self):
        return self.__name__

    def __eq__(self, other):
        return str(self) == str(other)


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
    (str, test_string),
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
        self.type_counts[unicode] = 0
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
            v = str(v)
        except UnicodeEncodeError:
            self.type_counts[unicode] += 1
            return unicode

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

                if test == str:
                    if v not in self.strings:
                        self.strings.append(v)

                    if (self.count < 1000 or self.date_successes != 0) and any((c in '-/:T') for c in v):
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

        # If it is more than 20% str, it's a str
        if self.type_ratios[str] > .2:
            return str, False

        # If more than 70% None, it's also a str, because ...
        #if self.type_ratios[None] > .7:
        #    return str, False

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

        # FIXME; need a better method of str/unicode that's compatible with Python3
        elif self.type_counts[str] > 0:
            num_type = str

        elif self.type_counts[unicode] > 0:
            num_type = unicode

        else:
            num_type = unknown

        if self.type_counts[str] > 0 and num_type != str:
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
        from collections import OrderedDict

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
                print i, value, e
                pass
                raise

    def run(self, source):
        for i, row in enumerate(iter(source)):
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
            o = '\n' + str(tabulate(results[1:], results[0], tablefmt='pipe'))
        else:
            o = ''

        return "TypeIntuiter " + o

    @staticmethod
    def promote_type(orig_type, new_type):
        """Given a table with an original type, decide whether a new determination of a new applicable type
        should overide the existing one"""

        try:
            orig_type = orig_type.__name__
        except AttributeError:
            pass

        try:
            new_type = new_type.__name__
        except AttributeError:
            pass

        type_precidence = ['unknown', 'int', 'float', 'date', 'time', 'datetime', 'str', 'unicode']

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

            d = dict(
                position=v.position,
                header=v.header,
                length=v.length,
                resolved_type=v.resolved_type_name,
                has_codes=v.has_codes,
                count=v.count,
                ints=v.type_counts.get(int, None),
                floats=v.type_counts.get(float, None),
                strs=v.type_counts.get(str, None),
                unicode=v.type_counts.get(unicode, None),
                nones=v.type_counts.get(None, None),
                datetimes=v.type_counts.get(datetime.datetime, None),
                dates=v.type_counts.get(datetime.date, None),
                times=v.type_counts.get(datetime.time, None),
                strvals=','.join(list(v.strings)[:20])
            )

            yield d


class ClusterHeaders(object):
    """Using Source table headers, cluster the source tables into destination tables"""

    def __init__(self, bundle = None):
        self._bundle = bundle
        self._headers = {}

    def match_headers(self, a, b):
        from difflib import SequenceMatcher, ndiff
        from collections import Counter

        c =  Counter(e[0] for e in ndiff(a,b) if e[0] != '?')

        same = c.get(' ',0)
        remove = c.get('-',0)
        add = c.get('+',0)

        return float(remove+add) / float(same)

    def match_headers_a(self, a, b):
        from difflib import SequenceMatcher

        for i, ca in enumerate(a):
            for j,cb in enumerate(b):
                r = SequenceMatcher(None, ca, cb).ratio()

                if r > .9:
                    print ca, cb
                    break

    def add_header(self, name, headers):
        self._headers[name] = headers

    def pairs(self):
        return  set([ (name1, name2) for name1 in list(self._headers) for name2 in list(self._headers) if name2 > name1])

    @classmethod
    def long_substr(cls,data):
        data = list(data)
        substr = ''
        if len(data) > 1 and len(data[0]) > 0:
            for i in range(len(data[0])):
                for j in range(len(data[0]) - i + 1):
                    if j > len(substr) and cls.is_substr(data[0][i:i + j], data):
                        substr = data[0][i:i + j]
        return substr

    @classmethod
    def is_substr(cls,find, data):
        if len(data) < 1 and len(find) < 1:
            return False
        for i in range(len(data)):
            if find not in data[i]:
                return False
        return True

    def cluster(self):

        pairs = self.pairs()

        results = []
        for a, b in pairs:
            results.append((round(self.match_headers(self._headers[a],self._headers[b]), 3), a, b))

        results = sorted(results, key = lambda r: r[0])

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
                    ns = set([a,b])
                    clusters.append(ns)

        d = { self.long_substr(c).strip('_'):sorted(c) for c in clusters }

        return d

class RowIntuiter(object):

    N_TEST_ROWS = 150

    type_map = { unicode: str, float: int }

    def __init__(self):
        import re

        self.comment_lines = []
        self.header_lines = []
        self.start_line = 0
        self.end_line = 0

        self.data_pattern_source = None

        self.patterns = (
            ('B', re.compile(r'^_+$')),
            ('C', re.compile(r'^XX_+$')),
            ('C', re.compile(r'^X_+$')),
            ('H', re.compile(r'^X+$')),
            ('H', re.compile(r"(?:X_)")),

        )

        self.test_rows = []

    def picture(self, row):
        """Create a simplified character representation of the data row, which can be pattern matched
        with a regex """

        template = '_Xn'
        types = (type(None), str, int)

        def guess_type(v):

            v = unicode(v).strip()

            if not bool(v):
                return type(None)

            for t in (float, int, str, unicode):
                try:
                    return type(t(v))
                except:
                    pass

        def p(e):
            try:
                t = guess_type(e)
                tm = self.type_map.get(t,t)
                return template[types.index(tm)]
            except ValueError:
                raise ValueError("Type '{}'/'{}' not in the types list: {}".format(t, tm, types))

        return ''.join( p(e) for e in row)

    def _data_pattern_source(self, rows, change_limit = 5):

        l = max(len(row) for row in rows)  # Length of longest row

        patterns = [set() for _ in range(l)]

        contributors = 0 # Number  of rows that contributed to pattern.

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
        import re

        tests = 50
        test_rows = 20

        def try_tests(tests, test_rows, rows):
            # Look for the first row where you can generate a data pattern that does not have a large number of changes
            # in subsequent rows.
            for i in range(tests):

                max_changes = len(rows[0])/4

                pattern_source, contributors, l = self._data_pattern_source(rows[i:i+test_rows], max_changes)

                if contributors > test_rows*.75:
                    return pattern_source

        pattern_source = try_tests(tests, test_rows, rows)

        # FIXME. Need to gracefully handle case of not finding a patterm.
        assert pattern_source

        pattern = re.compile(pattern_source)

        return pattern, pattern_source

    def classify(self, rows):

        import re

        header_rows = []
        found_header = False

        data_pattern_skip_rows = min(30, len(rows)-10)

        data_pattern, self.data_pattern_source = self.data_pattern(rows[data_pattern_skip_rows:])

        patterns = [('D',data_pattern)] + list(self.patterns)

        def match(picture):
            for l, r in patterns:
                if r.search(picture):
                    return l

            return False

        for i, row in enumerate(rows):

            picture = self.picture(row)

            label = match(picture)

            try:
                if label != 'B' and len(re.search('_+',picture).group(0)) > len(row)/2:
                    label = 'C'
            except AttributeError:
                pass # re not matched

            if not found_header and label == 'H':
                found_header = True

            if label == False and not found_header:
                # Could be a really wacky header
                found_header = True
                label = 'H'

            # print label, picture, row

            if label == 'C':
                self.comment_lines.append(i)

            elif label == 'H':
                self.header_lines.append(i)
                header_rows.append(row)

            elif label == 'D':
                self.start_line = i
                self.headers = self.coalesce_headers(header_rows)
                break

    def find_end(self, end_rows):

        raise NotImplementedError()

        import re
        from itertools import dropwhile, ifilterfalse
        from operator import itemgetter

        pattern = re.compile(self.data_pattern_source)

        # Return the first row where the pattern fails.
        try:
            pos, row = min(ifilterfalse(lambda row: pattern.match(self.picture(row)),
                                        enumerate(end_rows)), key=itemgetter(1))

            for row in end_rows:
                print self.picture(row), bool(pattern.match(self.picture(row)))

            rpos = pos - len(end_rows) - 1

            try:
                end_rows[rpos] # Check if rpos is in range
                return rpos
            except:
                return None

        except ValueError:
            return -1 # Signal that the last element is the end row.



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

            headers = [' '.join(unicode(col_val).strip() if col_val else '' for col_val in col_set)
                       for col_set in zip(*header_lines)]

            headers = [h.strip() for h in headers]

            return headers

        elif len(header_lines) > 0:
            return header_lines[0]

        else:
            return []

    def run(self, source):
        from itertools import islice

        self.test_rows = list(islice(iter(source), self.N_TEST_ROWS))

        self.classify(self.test_rows )

        return self


