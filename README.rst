Ambry Partition Message Pack File
=================================

This module supports the Ambry ETL framework by providing a file format for storing data and a collection
of import routines for other file formats

Source File Configuration
-------------------------

Parameters that can be set on a source file.

- url. The URL of the source file. If the URL has a fragment ( '#' ) the fragment represents a file inside of a zip archive
- segment. A number that indicates which worksheet to use in an Excel spreadsheet.
- header_lines. A comma seperated list of line numbers that should be used for the column headers
- urltype. If zip, indicates that the URL is for a zip file, for zip file that don't end in a 'zip' extension.
- filetype. A file extension to use for the file.
- encoding. A python encoding name. If missing, defaults to 'ascii', and is most often set to 'utf8'


