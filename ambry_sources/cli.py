# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


import argparse
from .mpf import MPRowsFile
from itertools import islice
import tabulate
from __meta__ import __version__

def make_arg_parser(parser=None):


    if not parser:
        parser = argparse.ArgumentParser(
            prog='ampr',
            description='Ambry Message Pack Rows file access version:'.format(__version__))

    parser.add_argument('-m', '--meta', action='store_true',
                        help='Show metadata')
    parser.add_argument('-s', '--schema', action='store_true',
                        help='Show the schema')
    parser.add_argument('-S', '--stats', action='store_true',
                        help='Show the statistics')
    parser.add_argument('-H', '--head', action='store_true',
                        help='Display the first 10 records. Will only display 80 chars wide')
    parser.add_argument('-T', '--tail', action='store_true',
                        help='Display the first last 10 records. Will only display 80 chars wide')
    parser.add_argument('-r', '--records', action='store_true',
                        help='Output the records in tabular format')
    parser.add_argument('-R', '--raw', action='store_true',
                        help='For the sample output, use the raw iterator')
    parser.add_argument('-j', '--json', action='store_true',
                        help='Output the entire file as JSON')
    parser.add_argument('-c', '--csv', help='Output the entire file as CSV')
    parser.add_argument('-l', '--limit', help='The number of rows to output for CSV or JSON')

    parser.add_argument('path', nargs=1, type=str, help='File path')

    return parser

def main(args=None):
    from operator import itemgetter
    from datetime import datetime

    if not args:
        parser = make_arg_parser()
        args = parser.parse_args()

    f = MPRowsFile(args.path[0])

    r = f.reader

    schema_fields = ['pos','name', 'type','resolved_type', 'description', 'start','width']
    schema_getter = itemgetter(*schema_fields)
    types_fields =  ['header', 'count','length',  'floats',  'ints', 'unicode',  'strs', 'dates',
                     'times', 'datetimes', 'nones', 'has_codes', 'strvals',  ]

    stats_fields_all = [ 'name', 'count', 'nuniques' , 'mean', 'min', 'p25', 'p50', 'p75' , 'max', 'std',
                    'uvalues', 'lom',  'skewness','kurtosis', 'flags', 'hist', 'text_hist']

    stats_fields = ['name', 'lom', 'count', 'nuniques', 'mean', 'min', 'p25', 'p50', 'p75',
                   'max', 'std', 'text_hist']

    stats_getter = itemgetter(*stats_fields)

    if args.csv:
        import unicodecsv as csv
        with f.reader as r:
            limit = int(args.limit) if args.limit else None
            with open(args.csv, 'wb') as out_f:
                w = csv.writer(out_f)
                w.writerow(r.headers)
                for i, row in enumerate(r.rows):
                    w.writerow(row)

                    if limit and i>= limit:
                        break


        return

    def pm(l,m):
        """Print, maybe"""
        if not m:
            return
        m = str(m).strip()
        if m:
            print "{:<12s}: {}".format(l,m)

    with f.reader as r:
        pm("MPR File",args.path[0])
        pm("Created", (r.meta['about']['create_time'] and datetime.fromtimestamp(r.meta['about']['create_time'])))
        pm("version", r.info['version'])
        pm("rows", r.info['rows'])
        pm("cols", r.info['cols'])
        pm("header_rows", r.info['header_rows'])
        pm("data_row", r.info['data_start_row'])
        pm("end_row", r.info['data_end_row'])

        ss = r.meta['source']
        pm("URL", ss['url'])
        pm("encoding", ss['encoding'])

    if args.schema:
        print "\nSCHEMA"
        with f.reader as r:
            print tabulate.tabulate((schema_getter(row.dict) for row in r.columns), schema_fields)

    if args.stats:
        with f.reader as r:
            print "\nSTATS"

            print tabulate.tabulate((stats_getter(row.dict) for row in r.columns), stats_fields)

    if args.head or args.tail:
        with f.reader as r:
            print ("\nHEAD" if args.head else "\nTAIL")
            MAX_LINE = 80
            ll = 0
            headers = []

            # Only show so may cols as will fit in an 80 char line.
            for h in r.headers:
                if len(' '.join(headers+[h])) > MAX_LINE:
                    break
                headers.append(h)

            itr = r.raw if args.raw else r.rows

            rows = []

            start, end = (None,10) if args.head else (r.n_rows-10, r.n_rows )

            slc = islice(itr,start,end)

            rows = [(i,)+row[:len(headers)] for i, row in enumerate(slc, start if start else 0)]

            print tabulate.tabulate(rows, ['#'] + headers)

    elif args.records:

        with f.reader as r:

            acc = []
            try:
                for i , row in enumerate(r.rows):

                    if i % 30 == 0:
                        print tabulate.tabulate(acc, r.headers)
                        acc = []
                    else:
                        acc.append(row)
            except KeyboardInterrupt:
                import sys
                sys.exit(0)




if __name__ == "__main__":
    main()