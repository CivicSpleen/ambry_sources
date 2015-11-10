# -*- coding: utf-8 -*-

from ambry_sources.exceptions import MissingCredentials
from ambry_sources.fetch import get_s3

from tests import TestBase

# Unit tests of the download module


class GetS3Test(TestBase):
    """ unit tests for get_s3 function. """

    def test_raises_TypeError_if_empty_account_accessor_given(self):
        try:
            get_s3('s3://example.com/file1.csv', None)
        except TypeError as exc:
            self.assertIn('must be callable', str(exc))

    def test_raises_TypeError_if_not_callable_account_accessor_given(self):
        try:
            get_s3('s3://example.com/file1.csv', {'key': 'value'})
        except TypeError as exc:
            self.assertIn('must be callable', str(exc))

    def test_raises_MissingCredentials_on_missed_access(self):
        try:
            get_s3(
                's3://example.com/file1.csv',
                lambda url: {'secret': 'secret'})
        except MissingCredentials as exc:
            self.assertIn('access', str(exc))

    def test_raises_MissingCredentials_on_missed_secret(self):
        try:
            get_s3(
                's3://example.com/file1.csv',
                lambda url: {'access': 'access'})
        except MissingCredentials as exc:
            self.assertIn('secret', str(exc))
