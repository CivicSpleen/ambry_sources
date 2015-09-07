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

parser.add_argument('-m', '--meta', help='Show metadata')
parser.add_argument('-s', '--sample', help='Sample the first 10 records')
parser.add_argument('-j', '--json', help='Output the entire file as JSON')

parser.add_argument('path', nargs='?', type=str, help='File path')

def main():
    args = parser.parse_args()

    f = MPRowsFile(args.path)

    r = f.reader

    print r.meta
    print

    print tabulate.tabulate(islice(r.rows,10), r.headers)



if __name__ == "__main__":
    main()