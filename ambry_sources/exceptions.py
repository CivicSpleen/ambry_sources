# -*- coding: utf-8 -*-


class VirtualTableError(Exception):
    pass

class ConfigurationError(Exception):
    pass


class DownloadError(Exception):
    pass


class RowIntuitError(Exception):
    pass


class MissingCredentials(Exception):

    def __init__(self, message, location=None, required_credentials=None):
        super(MissingCredentials, self).__init__(message)
        self.location = location
        self.required_credentials = required_credentials
