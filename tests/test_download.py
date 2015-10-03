# -*- coding: utf-8 -*-

from ambry_sources.download import get_s3

from tests import TestBase

# Unit tests of the download module


class GetS3Test(TestBase):
    """ unit tests for get_s3 function. """

    def test_raises_TypeError_if_empty_account_accessor_given(self):
        try:
            get_s3('s3://example.com/file1.csv', None)
        except TypeError as exc:
            self.assertIn('has to be callable', str(exc))

    def test_raises_TypeError_if_not_callable_account_accessor_given(self):
        try:
            get_s3('s3://example.com/file1.csv', {'key': 'value'})
        except TypeError as exc:
            self.assertIn('has to be callable', str(exc))

    def test_raises_ValueError_on_missed_access(self):
        try:
            get_s3(
                's3://example.com/file1.csv',
                lambda url: {'secret': 'secret'})
        except ValueError as exc:
            self.assertIn('has to contain not empty `access` key', str(exc))

    def test_raises_ValueError_on_missed_secret(self):
        try:
            get_s3(
                's3://example.com/file1.csv',
                lambda url: {'access': 'access'})
        except ValueError as exc:
            self.assertIn('has to contain not empty `secret` key', str(exc))
