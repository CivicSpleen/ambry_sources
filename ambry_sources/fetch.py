# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import functools
import hashlib
from os.path import join
import re
import ssl

from requests import HTTPError

import six
from six import binary_type, b
from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen

from fs.zipfs import ZipFS
from fs.s3fs import S3FS

from ambry_sources.exceptions import ConfigurationError, DownloadError, MissingCredentials
from ambry_sources.mpf import MPRowsFile
from ambry_sources.util import copy_file_or_flo, parse_url_to_dict

from .sources import GoogleSource, CsvSource, TsvSource, FixedSource, ExcelSource, PartitionSource,\
    SourceError, DelayedOpen, ShapefileSource


def get_source(spec, cache_fs,  account_accessor=None, clean=False, logger=None, callback=None):
    """
    Download a file from a URL and return it wrapped in a row-generating acessor object.

    :param spec: A SourceSpec that describes the source to fetch.
    :param cache_fs: A pyfilesystem filesystem to use for caching downloaded files.
    :param account_accessor: A callable to return the username and password to use for access FTP and S3 URLs.
    :param clean: Delete files in cache and re-download.
    :param logger: A logger, for logging.
    :param callback: A callback, called while reading files in download. signatire is f(read_len, total_len)

    :return: a SourceFile object.
    """

    # FIXME. urltype should be moved to reftype.
    url_type = spec.get_urltype()

    if url_type != 'gs': #FIXME. Need to clean up the logic for gs types.
        try:
            cache_path, download_time = download(spec.url, cache_fs, account_accessor,
                                                 clean=clean, logger=logger, callback=callback)
            spec.download_time = download_time
        except HTTPError as e:
            raise DownloadError("Failed to download {}; {}".format(spec.url, e))
    else:
        cache_path, download_time = None, None

    if url_type == 'zip':
        fstor = extract_file_from_zip(cache_fs, cache_path, spec.url, spec.file)
        file_type = spec.get_filetype(fstor.path)
    elif url_type == 'gs':
        fstor = get_gs(spec.url, spec.segment, account_accessor)
        file_type = 'gs'
    else:
        fstor = DelayedOpen(cache_fs, cache_path, 'rb')
        file_type = spec.get_filetype(fstor.path)

    spec.filetype = file_type

    TYPE_TO_SOURCE_MAP = {
        'gs': GoogleSource,
        'csv': CsvSource,
        'tsv': TsvSource,
        'fixed': FixedSource,
        'txt': FixedSource,
        'xls': ExcelSource,
        'xlsx': ExcelSource,
        'partition': PartitionSource,
        'shape': ShapefileSource}

    cls = TYPE_TO_SOURCE_MAP.get(file_type)
    if cls is None:
        raise SourceError(
            "Failed to determine file type for source '{}'; unknown type '{}' "
            .format(spec.name, file_type))

    return cls(spec, fstor)


def import_source(spec, cache_fs,  file_path=None, account_accessor=None):
    """Download a source and load it into an MPR file. """

    s = get_source(spec, cache_fs,  account_accessor)

    if not file_path:
        file_path = spec.name

    f = MPRowsFile(cache_fs, file_path)
    w = f.writer

    w.set_spec(spec)

    for row in s:
        w.insert_row(row)

    w.close()

    return f


def extract_file_from_zip(cache_fs, cache_path, url, fn_pattern = None):
    """
    For a zip archive, return the first file if no file_name is specified as a fragment in the url,
     or if a file_name is specified, use it as a regex to find a file in the archive

    :param cache_fs:
    :param cache_path:
    :param url:
    :return:
    """

    from fs.zipfs import ZipOpenError

    # FIXME Not sure what is going on here, but in multiproccessing mode,
    # the 'try' version of opening the file can fail with an error about the file being missing or corrupy
    # but the second successedes. However, the second will faile in test environments that have a memory cache.
    try:
        fs = ZipFS(cache_fs.open(cache_path, 'rb'))
    except ZipOpenError:
        fs = ZipFS(cache_fs.getsyspath(cache_path))

    fstor = None

    def walk_all(fs):
        return [join(e[0], x) for e in fs.walk() for x in e[1]]

    if not fn_pattern and '#' in url:
        _, fn_pattern = url.split('#')

    if not fn_pattern:
        first = walk_all(fs)[0]
        fstor = DelayedOpen(fs, first, 'rU', container=(cache_fs, cache_path))

    else:

        for file_name in walk_all(fs):

            if '_MACOSX' in file_name:
                continue

            if re.search(fn_pattern, file_name):

                fstor = DelayedOpen(fs, file_name, 'rb', container=(cache_fs, cache_path))
                break

        if not fstor:
            raise ConfigurationError("Failed to get file for pattern '{}' from archive {}".format(fn_pattern, fs))

    return fstor

def _download(url, cache_fs, cache_path, account_accessor, logger, callback ):

    import requests
    import os

    assert callback is not None

    if url.startswith('s3:'):
        s3 = get_s3(url, account_accessor)
        pd = parse_url_to_dict(url)

        with cache_fs.open(cache_path, 'wb') as fout:
            with s3.open(pd['path'], 'rb') as fin:
                copy_file_or_flo(fin, fout, cb = callback)

    elif url.startswith('ftp:'):
        import shutil
        from contextlib import closing

        with closing(urlopen(url)) as fin:
            with cache_fs.open(cache_path, 'wb') as fout:
                shutil.copyfileobj(fin, fout)
    else:

        r = requests.get(url, stream=True)
        r.raise_for_status()

        # Requests will auto decode gzip responses, but not when streaming. This following
        # monkey patch is recommended by a core developer at
        # https://github.com/kennethreitz/requests/issues/2155
        if r.headers.get('content-encoding') == 'gzip':
            r.raw.read = functools.partial(r.raw.read, decode_content=True)

        with cache_fs.open(cache_path, 'wb') as f:
            copy_file_or_flo(r.raw, f, cb = callback)

        assert cache_fs.exists(cache_path)


class _NoOpFileLock(object):
    """No Op for pyfilesystem caches where locking wont work"""
    def __init__(self, lf):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            raise exc_val

    def acquire(self):
        pass

    def release(self):
        pass


def download(url, cache_fs, account_accessor=None, clean=False, logger=None, callback=None):
    """
    Download a URL and store it in the cache.

    :param url:
    :param cache_fs:
    :param account_accessor: callable of one argument (url) returning dict with credentials.
    :param clean: Remove files from cache and re-download
    :param logger:
    :param callback:
    :return:
    """
    import os.path
    import requests
    from fs.errors import NoSysPathError, ResourceInvalidError

    import time

    parsed = urlparse(url)

    # Create a name for the file in the cache, based on the URL
    cache_path = os.path.join(parsed.netloc, parsed.path.strip('/'))

    # If there is a query, hash it and add it to the path
    if parsed.query:
        hash = hashlib.sha224(parsed.query).hexdigest()
        cache_path = os.path.join(cache_path, hash)

    try:
        from  filelock import FileLock
        lock = FileLock(cache_fs.getsyspath(cache_path + '.lock'))

    except NoSysPathError:
        # mem: caches, and others, don't have sys paths.
        # FIXME should check for MP operation and raise if there would be
        # contention. Mem  caches are only for testing with single processes
        lock = _NoOpFileLock()

    if not cache_fs.exists(cache_path):
        cache_fs.makedir(os.path.dirname(cache_path), recursive=True, allow_recreate=True)

    with lock:
        if cache_fs.exists(cache_path):
            if clean:
                try:
                    cache_fs.remove(cache_path)
                except ResourceInvalidError:
                    pass  # Well, we tried.
            else:
                return cache_path, None

        try:
            _download(url, cache_fs, cache_path, account_accessor, logger, callback)

            return cache_path, time.time()

        except (KeyboardInterrupt, Exception):
            # This is really important -- its really bad to have partly downloaded
            # files being confused with fully downloaded ones.
            # FIXME. Should also handle signals. deleteing partly downloaded files is important.
            # Maybe should have a sentinel file, or download to another name and move the
            # file after done.
            if cache_fs.exists(cache_path):
                cache_fs.remove(cache_path)

            raise

    assert False, "Should never get here"




def get_s3(url, account_accessor):
    """ Gets file from s3 storage.

    Args:
        url (str): url of the file
        account_accessor (callable): callable returning dictionary with s3 credentials (access and secret
            at least)

    Example:
        get_s3('s3://example.com/file1.csv', lambda url: {'access': '<access>': 'secret': '<secret>'})

    Returns:
        S3FS instance (file-like):
    """

    # TODO: Hack the pyfilesystem fs.opener file to get credentials from a keychain
    # The monkey patch fixes a bug: https://github.com/boto/boto/issues/2836

    _old_match_hostname = ssl.match_hostname

    # FIXME. This issue is possibly better handled with https://pypi.python.org/pypi/backports.ssl_match_hostname
    def _new_match_hostname(cert, hostname):
        if hostname.endswith('.s3.amazonaws.com'):
            pos = hostname.find('.s3.amazonaws.com')
            hostname = hostname[:pos].replace('.', '') + hostname[pos:]
        return _old_match_hostname(cert, hostname)

    ssl.match_hostname = _new_match_hostname

    pd = parse_url_to_dict(url)

    if account_accessor is None or not six.callable(account_accessor):
        raise TypeError('account_accessor argument must be callable of one argument returning dict.')

    account = account_accessor(pd['netloc'])
    # Direct access to the accounts file yeilds 'access', but in the Accounts ORM object, its 'access_key'
    aws_access_key = account.get('access', account.get('access_key'))
    aws_secret_key = account.get('secret')

    missing_credentials = []
    if not aws_access_key:
        missing_credentials.append('access')
    if not aws_secret_key:
        missing_credentials.append('secret')

    if missing_credentials:
        raise MissingCredentials(
            'dict returned by account_accessor callable for {} must contain not empty {} key(s)'
            .format(pd['netloc'], ', '.join(missing_credentials)),
            location=pd['netloc'],required_credentials=['access', 'secret'], )

    s3 = S3FS(
        bucket=pd['netloc'],
        # prefix=pd['path'],
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key
    )

    # ssl.match_hostname = _old_match_hostname

    return s3


def get_gs(url, segment, account_acessor):

    import gspread
    from oauth2client.client import SignedJwtAssertionCredentials

    json_key = account_acessor('google_spreadsheets')

    scope = ['https://spreadsheets.google.com/feeds']

    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

    spreadsheet_key = url.replace('gs://', '')

    gc = gspread.authorize(credentials)

    sh = gc.open_by_key(spreadsheet_key)

    return sh.worksheet(segment)
