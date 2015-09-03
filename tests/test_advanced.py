# -*- coding: utf-8 -*-

from .context import ambry_pmpf

import unittest


class AdvancedTestSuite(unittest.TestCase):
    """Advanced test cases."""

    def test_thoughts(self):
        ambry_pmpf.core.hello()


if __name__ == '__main__':
    unittest.main()
