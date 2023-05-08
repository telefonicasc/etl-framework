# -*- coding: utf-8 -*-
#
# Copyright 2023 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
#
# This file is part of tc_etl_lib
#
# tc_etl_lib is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# tc_etl_lib is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
# General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with IoT orchestrator. If not, see http://www.gnu.org/licenses/.

'''
Normalizer tests
'''

import unittest
from tc_etl_lib import normalizer


class TestNormalizer(unittest.TestCase):
    '''Tests for sqlFileStore'''

    def do_test(self, replacement="-", override=None, input="", expected=""):
        '''test Normalizer with the given options dict'''
        norm = normalizer(replacement=replacement, override=override)
        result = norm(input)
        self.assertEqual(result, expected)

    def test_default_behaviour(self):
        self.do_test(
            replacement="-",
            override=None,
            input="text (with & specials) > áéíóúñ",
            expected="text--with---specials----aeioun"
        )

    def test_different_replacement(self):
        self.do_test(
            replacement="_",
            override=None,
            input="text (with & specials) > áéíóúñ",
            expected="text__with___specials____aeioun"
        )

    def test_space_override(self):
        self.do_test(
            replacement="-",
            override={" ": "+"},
            input="text (with & specials) > áéíóúñ",
            expected="text+-with+-+specials-+-+aeioun"
        )

    def test_forbidden_override(self):
        self.do_test(
            replacement="-",
            override={">": "+"},
            input="text (with & specials) > áéíóúñ",
            expected="text--with---specials--+-aeioun"
        )

    def test_forbidden_remove_some(self):
        self.do_test(
            replacement="-",
            override= {
                "(": None,
                ")": None,
            },
            input="text (with & specials) > áéíóúñ",
            expected="text-with---specials---aeioun"
        )

    def test_space_remove(self):
        self.do_test(
            replacement="-",
            override= {" ": None },
            input="text (with & specials) > áéíóúñ",
            expected="text-with-specials--aeioun"
        )

    def test_forbidden_remove_all(self):
        self.do_test(
            replacement="",
            override={ " ": "-" },
            input="text (with & specials) > áéíóúñ",
            expected="text-with-specials-aeioun"
        )

    def test_remove_all(self):
        self.do_test(
            replacement="",
            override=None,
            input="text (with & specials) > áéíóúñ",
            expected="textwithspecialsaeioun"
        )
