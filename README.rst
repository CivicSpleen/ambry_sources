Ambry Partition Message Pack File
=================================

This module supports the Ambry ETL framework by providing a file format for storing data and a collection
of import routines for other file formats

The Message Pack Rows (MPR) file format consists of a compressed collection of arrays, in message pack, followed by a
dictionary of metadata, also in Message Pack. The format efficiently stores tabular data and associates it with
metadata, with a few special features for use with data that can come from a variety of sources.

For instance, data a Fixed Width text file may not have a the column titles -- headers -- in the first row, so the
file can store a schema in metadata. Other files, such as those that originate in Excel, may not have their headers
on the first so the MPR file can specify a later row to be the start of data.

This module also includes classes for guessing the datatypes of each column, determining where the first row of data
begins, and computing statistics.

Command Line Interface
----------------------

THe module installs a command line program ``ampr`` which can be used to inspect MPR files. Run ``ambry -h`` for help.

Source File Configuration
-------------------------

Parameters that can be set on a source file.

- url. The URL of the source file. If the URL has a fragment ( '#' ) the fragment represents a file inside of a zip archive
- segment. A number that indicates which worksheet to use in an Excel spreadsheet.
- header_lines. A comma seperated list of line numbers that should be used for the column headers
- urltype. If zip, indicates that the URL is for a zip file, for zip file that don't end in a 'zip' extension.
- filetype. A file extension to use for the file.
- encoding. A python encoding name. If missing, defaults to 'ascii', and is most often set to 'utf8'