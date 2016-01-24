# -*- coding: utf-8 -*-
"""
Copyright (c) 2015 Civic Knowledge. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""


class DelayedOpen(object):
    """A Lightweight wrapper to delay opening a PyFilesystem object until is it used. It is needed because
    The open() command on a filesystem directory, to produce the file object, also opens the file
    """
    def __init__(self, fs, path, mode='r', container=None,  account_accessor=None):

        self._fs = fs
        self._path = path
        self._mode = mode
        self._container = container
        self._account_accessor = account_accessor

    def open(self, mode=None, encoding=None):
        return self._fs.open(self._path, mode if mode else self._mode, encoding=encoding)

    @property
    def syspath(self):
        return self._fs.getsyspath(self._path)

    def sub_cache(self):
        """Return a fs directory associated with this file """
        import os.path

        if self._container:
            fs, container_path = self._container

            dir_path = os.path.join(container_path + '_')

            fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return fs.opendir(dir_path)

        else:

            dir_path = os.path.join(self._path+'_')

            self._fs.makedir(dir_path, recursive=True, allow_recreate=True)

            return self._fs.opendir(dir_path)

    @property
    def path(self):
        return self._path

    def __str__(self):

        from fs.errors import NoSysPathError

        try:
            return self.syspath
        except NoSysPathError:
            return "Delayed Open: {}; {} ".format(str(self._fs), str(self._path))


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
        self.__pos_map = {e: i for i, e in enumerate(keys)}
        self.__initialized = True

    @property
    def row(self):
        return object.__getattribute__(self, '_RowProxy__row')

    def set_row(self, v):
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
                try:
                    return self.__row[key]
                except IndexError:
                    raise KeyError("Failed to get value for integer key '{}' ".format(key))
            else:
                try:
                    return self.__row[self.__pos_map[key]]
                except IndexError:
                    raise IndexError("Failed to get value for non-int key '{}', resolved to position {} "
                                   .format(key, self.__pos_map[key]))
                except KeyError:
                    raise KeyError("Failed to get value for non-int key '{}' ".format(key))


    def __setattr__(self, key, value):

        if '_RowProxy__initialized' not in self.__dict__:
            return object.__setattr__(self, key, value)

        else:
            self.__row[self.__pos_map[key]] = value

    def __getattr__(self, key):
        try:
            return self.__row[self.__pos_map[key]]
        except KeyError:
            raise KeyError("Failed to find key '{}'; has {}".format(key, self.__keys))

    def __delitem__(self, key):
        raise NotImplementedError()

    def __iter__(self):
        return iter(self.__keys)

    def __len__(self):
        return len(self.__keys)

    @property
    def dict(self):
        return dict(zip(self.__keys, self.__row))

    def copy(self):
        return type(self)(self.__keys).set_row(self.row)

    def keys(self):
        return self.__keys

    def values(self):
        return self.__row

    def items(self):
        return zip(self.__keys, self.__row)

    # The final two methods aren't required, but nice for demo purposes:
    def __str__(self):
        """ Returns simple dict representation of the mapping. """
        return str(self.dict)

    def __repr__(self):
        return self.dict.__repr__()
