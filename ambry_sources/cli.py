# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from . import __version__
import argparse
from .mpf import MPRowsFile
from itertools import islice
import tabulate

parser = argparse.ArgumentParser(
    prog='ampr',
    description='Ambry Message Pack Rows file access version:'.format(__version__))

parser.add_argument('-l', '--ls', dest='command',
                    help='List the contents of the file')

parser.add_argument('-m', '--meta', action='store_true',
                    help='Show metadata')
parser.add_argument('-s', '--schema', action='store_true',
                    help='Show the schema')
parser.add_argument('-S', '--stats', action='store_true',
                    help='Show the schema')
parser.add_argument('-r', '--sample', action='store_true',
                    help='Sample the first 10 records')
parser.add_argument('-j', '--json', action='store_true',
                    help='Output the entire file as JSON')

parser.add_argument('path', nargs='?', type=str, help='File path')


def main():
    from operator import itemgetter
    from datetime import datetime
    args = parser.parse_args()

    f = MPRowsFile(args.path)

    r = f.reader

    schema_fields = ['pos','name', 'type','resolved_type', 'description']
    schema_getter = itemgetter(*schema_fields)
    types_fields =  ['header', 'count','length',  'floats',  'ints', 'unicode',  'strs', 'dates',
                     'times', 'datetimes', 'nones', 'has_codes', 'strvals',  ]

    stats_fields_all = [ 'name', 'count', 'nuniques' , 'mean', 'min', 'p25', 'p50', 'p75' , 'max', 'std',
                    'uvalues', 'lom',  'skewness','kurtosis', 'flags', 'hist', 'text_hist']

    stats_fields = ['name', 'lom', 'count', 'nuniques', 'mean', 'min', 'p25', 'p50', 'p75',
                   'max', 'std', 'text_hist']

    stats_getter = itemgetter(*stats_fields)

    def pm(l,m):
        """Print, maybe"""
        if not m:
            return
        m = str(m).strip()
        if m:
            print "{:<10s}: {}".format(l,m)

    pm("MPR File",args.path)
    pm("Created", (r.meta['about']['create_time'] and datetime.fromtimestamp(r.meta['about']['create_time'])))

    ss = r.meta['source']

    pm("URL", ss['url'])
    pm("encoding", ss['encoding'])

    if args.schema:
        print "\nSCHEMA"

        print tabulate.tabulate((schema_getter(row) for row in f.schema), schema_fields)


    if args.stats:
        print "\nSTATS"
        stats = [r.meta['stats'].get(row['name']) for row in r.meta['schema']]
        print tabulate.tabulate((stats_getter(row) for row in stats), stats_fields)


    if args.sample:
        print "\nSAMPLE"
        MAX_LINE = 80
        ll = 0
        headers = []
        for h in r.headers:
            if len(' '.join(headers+[h])) > MAX_LINE:
                break
            headers.append(h)


        print tabulate.tabulate(islice([ r[:len(headers)] for r in r.rows],10), headers)



if __name__ == "__main__":
    main()