# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__version__ = '0.0.1'
__author__ = 'eric@civicknowledge.com'

from .download import get_source, import_source
from mpf import MPRowsFile
from sources import RowProxy

import logging
#FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig()

if __name__ == "__main__":
    from .cli import main
    main()