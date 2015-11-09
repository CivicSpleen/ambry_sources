# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

from  .__meta__ import *

from .fetch import get_source, import_source, download, extract_file_from_zip
from .sources.spec import ColumnSpec, SourceSpec
from .mpf import MPRowsFile
from .sources import RowProxy

import logging

# FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig()

if __name__ == "__main__":
    from .cli import main
    main()
