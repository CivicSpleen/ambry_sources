# -*- coding: utf-8 -*-

from ambry_sources import get_source

from fs.zipfs import ZipFS
from fs.opener import fsopendir

from tests import TestBase

# Tests https://github.com/CivicKnowledge/ambry_sources/issues/10 issue.


class Test(TestBase):
    """ shapefiles (*.shp) accessor tests. """

    def test_bad_row_intuition(self):
        from ambry_sources.mpf import MPRowsFile
        from ambry_sources.sources.spec import SourceSpec, ColumnSpec

        cache_fs = fsopendir('temp://')

        spec = SourceSpec('http://www2.census.gov/acs2009_1yr/summaryfile/Entire_States/Arizona.zip',
                          file='g2009.*\.txt',
                          filetype='fixed',
                          name='geofile',
                          encoding='latin1',
                          )

        spec.columns = [ColumnSpec(position=1,width=6,name="fileid",start=1),
                        ColumnSpec(position=2,width=2,name="stusab",start=7),
                        ColumnSpec(position=3,width=3,name="sumlevel",start=9),
                        ColumnSpec(position=4,width=2,name="component",start=12),
                        ColumnSpec(position=5,width=7,name="logrecno",start=14),
                        ColumnSpec(position=6,width=1,name="us",start=21),
                        ColumnSpec(position=7,width=1,name="region",start=22),
                        ColumnSpec(position=8,width=1,name="division",start=23),
                        ColumnSpec(position=9,width=2,name="statece",start=24),
                        ColumnSpec(position=10,width=2,name="state",start=26),
                        ColumnSpec(position=11,width=3,name="county",start=28),
                        ColumnSpec(position=12,width=5,name="cousub",start=31),
                        ColumnSpec(position=13,width=5,name="place",start=36),
                        ColumnSpec(position=14,width=6,name="tract",start=41),
                        ColumnSpec(position=15,width=1,name="blkgrp",start=47),
                        ColumnSpec(position=16,width=5,name="concit",start=48),
                        ColumnSpec(position=17,width=4,name="aianhh",start=53),
                        ColumnSpec(position=18,width=5,name="aianhhfp",start=57),
                        ColumnSpec(position=19,width=1,name="aihhtli",start=62),
                        ColumnSpec(position=20,width=3,name="aitsce",start=63),
                        ColumnSpec(position=21,width=5,name="aits",start=66),
                        ColumnSpec(position=22,width=5,name="anrc",start=71),
                        ColumnSpec(position=23,width=5,name="cbsa",start=76),
                        ColumnSpec(position=24,width=3,name="csa",start=81),
                        ColumnSpec(position=25,width=5,name="metdiv",start=84),
                        ColumnSpec(position=26,width=1,name="macc",start=89),
                        ColumnSpec(position=27,width=1,name="memi",start=90),
                        ColumnSpec(position=28,width=5,name="necta",start=91),
                        ColumnSpec(position=29,width=3,name="cnecta",start=96),
                        ColumnSpec(position=30,width=5,name="nectadiv",start=99),
                        ColumnSpec(position=31,width=5,name="ua",start=104),
                        ColumnSpec(position=33,width=2,name="cdcurr",start=114),
                        ColumnSpec(position=34,width=3,name="sldu",start=116),
                        ColumnSpec(position=35,width=3,name="sldl",start=119),
                        ColumnSpec(position=39,width=5,name="submcd",start=136),
                        ColumnSpec(position=40,width=5,name="sdelm",start=141),
                        ColumnSpec(position=41,width=5,name="sdsec",start=146),
                        ColumnSpec(position=42,width=5,name="sduni",start=151),
                        ColumnSpec(position=43,width=1,name="ur",start=156),
                        ColumnSpec(position=44,width=1,name="pci",start=157),
                        ColumnSpec(position=47,width=5,name="puma5",start=169),
                        ColumnSpec(position=49,width=40,name="geoid",start=179),
                        ColumnSpec(position=50,width=200,name="name",start=219)]

        s = get_source(spec, cache_fs)

        f = MPRowsFile(cache_fs, spec.name)

        if f.exists:
            f.remove()

        f.load_rows(s)

        self.assertEqual(119, f.reader.info['data_end_row'])