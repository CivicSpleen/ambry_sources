"""
Created on Jun 22, 2012

@author: eric
"""

import unittest

import psycopg2

from six.moves.urllib.parse import urlparse
from six.moves import input as six_input


POSTGRES_SCHEMA_NAME = 'library'
POSTGRES_PARTITION_SCHEMA_NAME = 'partitions'
MISSING_POSTGRES_CONFIG_MSG = 'PostgreSQL is not configured properly. Add postgresql-test '\
    'to the database config of the ambry config.'
SAFETY_POSTFIX = 'ab1efg2'  # Prevents wrong database dropping.


class TestBase(unittest.TestCase):
    pass


class PostgreSQLTestBase(TestBase):
    """ Base class for database tests who requires postgresql database. """

    def setUp(self):
        super(PostgreSQLTestBase, self).setUp()
        # Create database and populate required fields.
        self._create_postgres_test_db()

    def tearDown(self):
        super(PostgreSQLTestBase, self).tearDown()
        self._drop_postgres_test_db()

    @classmethod
    def _drop_postgres_test_db(cls):
        # drop test database
        if hasattr(cls, 'pg_test_db_data'):
            # connect to postgres database and drop test database.
            with psycopg2.connect(**cls.pg_postgres_db_data) as conn:
                with conn.cursor() as cursor:
                    # we have to close opened transaction.
                    cursor.execute('commit;')
                    test_db_name = cls.pg_test_db_data['database']
                    assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'
                    cursor.execute('DROP DATABASE {};'.format(test_db_name))
                    cursor.execute('commit')
        else:
            # no database were created.
            pass

    @classmethod
    def _create_postgres_test_db(cls):
        """ FIXME: """
        database = 'ambry'
        user = 'ambry'
        password = 'secret'
        host = '127.0.0.1'
        test_db_name = '{}_test_{}'.format(database, SAFETY_POSTFIX)
        postgres_db_name = 'postgres'

        # connect to postgres database because we need to create database for tests.
        with psycopg2.connect(host=host, database=postgres_db_name, user=user, password=password) as conn:
            with conn.cursor() as curs:
                # we have to close opened transaction.
                curs.execute('commit;')

                # drop test database created by previuos run (control + c case).
                if cls.postgres_db_exists(test_db_name, curs):
                    assert test_db_name.endswith(SAFETY_POSTFIX), 'Can not drop database without safety postfix.'
                    while True:
                        delete_it = six_input(
                            '\nTest database with {} name already exists. Can I delete it (Yes|No): '.format(test_db_name))
                        if delete_it.lower() == 'yes':
                            try:
                                curs.execute('DROP DATABASE {};'.format(test_db_name))
                                curs.execute('commit')
                            except:
                                curs.execute('rollback')
                            break

                        elif delete_it.lower() == 'no':
                            break

                #
                # check for test template with required extensions.

                TEMPLATE_NAME = 'template0_ambry_test'
                cls.test_template_exists = cls.postgres_db_exists(TEMPLATE_NAME, curs)

                if not cls.test_template_exists:
                    raise unittest.SkipTest(
                        'Tests require custom postgres template db named {}. '
                        'See DEVEL-README.md for details.'.format(TEMPLATE_NAME))

                query = 'CREATE DATABASE {} OWNER {} TEMPLATE {} encoding \'UTF8\';'\
                    .format(test_db_name, user, TEMPLATE_NAME)
                curs.execute(query)
                curs.execute('commit')

        # reconnect to test db and create schemas needed by ambry_sources.
        with psycopg2.connect(host=host, database=test_db_name, user=user, password=password) as conn:
            with conn.cursor() as curs:
                # we have to close opened transaction.
                curs.execute('commit;')
                curs.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_SCHEMA_NAME))
                curs.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(POSTGRES_PARTITION_SCHEMA_NAME))

                if not cls.postgres_extension_installed('pg_trgm', curs):
                    raise unittest.SkipTest(
                        'Can not find template with pg_trgm extension. See DEVEL-README.md for details.')

                if not cls.postgres_extension_installed('multicorn', curs):
                    raise unittest.SkipTest(
                        'Can not find template with multicorn extension. See DEVEL-README.md for details.')

        cls.pg_postgres_db_data = {
            'database': postgres_db_name,
            'user': user,
            'host': host,
            'password': password
        }

        cls.pg_test_db_data = {
            'database': test_db_name,
            'user': user,
            'host': host,
            'password': password
        }
        return (cls.pg_postgres_db_data, cls.pg_test_db_data)

    @classmethod
    def postgres_db_exists(cls, db_name, cursor):
        """ Returns True if database with given name exists in the postgresql. """
        cursor.execute('SELECT 1 FROM pg_database WHERE datname=%s;', [db_name])
        result = cursor.fetchall()
        return result == [(1,)]

    @classmethod
    def postgres_extension_installed(cls, extension, cursor):
        """ Returns True if extension with given name exists in the postgresql. """
        cursor.execute('SELECT 1 FROM pg_extension WHERE extname=%s', [extension])
        result = cursor.fetchall()
        return result == [(1,)]
