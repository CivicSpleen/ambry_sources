# -*- coding: utf-8 -*-
"""

Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

__version__ = '0.0.1'

from six.moves.urllib.parse import urlparse
from six.moves.urllib.request import urlopen

from download import get_source


class SourceFile(object):
    """Base class for accessors that generate rows from a soruce file """

    def __init__(self, fstor):
        """

        :param flo: A File-like object for the file, already opened.
        :return:
        """

        # The fstor was a bit like a functor that delayed opening the filesystme object,
        # but now it looks like a remant that can be factored out.
        self._fstor = fstor

    def __iter__(self):
        rg = self._get_row_gen()
        self.start()
        for i, row in enumerate(rg):
            if i == 0:
                self.headers = row

            yield row

        self.finish()



    def _get_row_gen(self):
        pass

    def start(self):
        pass

    def finish(self):
        pass


class RowProxy(object):
    '''
    A dict-like accessor for rows which holds a constant header for the keys. Allows for faster access than
    constructing a dict, and also provides attribute access

    >>> header = list('abcde')
    >>> rp = RowProxy(header)
    >>> for i in range(10):
    >>>     row = [ j for j in range(len(header)]
    >>>     rp.set_row(row)
    >>>     print rp['c']

    '''

    def __init__(self, keys):

        self.__keys = keys
        self.__row = [None] * len(keys)
        self.__pos_map = { e:i for i, e in enumerate(keys)}
        self.__initialized = True

    @property
    def row(self):
        return object.__getattribute__(self, '_RowProxy__row')

    def set_row(self,v):
        object.__setattr__(self, '_RowProxy__row', v)
        return self

    @property
    def headers(self):
        return self.__getattribute__('_RowProxy__keys')

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.__row[key] = value
        else:
            self.__row[self.__pos_map[key]] = value

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.__row[key]
        else:
            return self.__row[self.__pos_map[key]]

    def __setattr__(self, key, value):

        if not self.__dict__.has_key('_RowProxy__initialized'):
            return object.__setattr__(self, key, value)

        else:
            self.__row[self.__pos_map[key]] = value

    def __getattr__(self, key):

        return self.__row[self.__pos_map[key]]

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.__keys)

    def __len__(self):
        return len(self.__keys)

    @property
    def dict(self):
        return dict(zip(self.__keys, self.__row))

    # The final two methods aren't required, but nice for demo purposes:
    def __str__(self):
        '''returns simple dict representation of the mapping'''
        return str(self.dict)

    def __repr__(self):
        return self.dict.__repr__()

class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
    def __init__(self, fs, path, mode='r', from_cache=False, account_accessor=None):

        self._fs = fs
        self._path = path
        self._mode = mode
        self._account_accessor = account_accessor

        self.from_cache = from_cache

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    def syspath(self):
        return self._fs.getsyspath(self._path)

    @property
    def path(self):
        return self._path

    def __str__(self):

        from fs.errors import NoSysPathError

        try:
            return self.syspath()
        except NoSysPathError:
            return "Delayed Open: source = {}; {}; {} ".format(self._source.name, str(self._fs),str(self._path))