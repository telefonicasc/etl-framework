#!/usr/bin/env python
# coding: utf-8

import unicodedata
import re

from typing import Mapping, Optional

_whitespace_re = re.compile(r"\s+")

class normalizer:
    """
    Normalizer is a class that will normalize unicode strings to
    valid NGSI entity IDs. Normalization rules are at:

    https://github.com/telefonicaid/fiware-orion/blob/master/doc/manuals/orion-api.md#general-syntax-restrictions

    Normalizers have a __call__ function that takes an input string and:

    - Turn accented characters (á, é, í, ó, u) into unaccented variants.
    - Remove any other unicode character not available in ascii
    - Remove ascii control codes
    - Replace forbidden characters '&', '?', '/', '#' '<', '>', '"', ''', '=', ';', '(', ')'
      with the replacement character (default "-", can be changed in the constructor)
    - Merges consecutive whitespace and replaces it with the replacement character

    You can also set a different replacement character for a specific forbidden
    character, by adding the translation to the `override` optional argument of the
    constructor.

    E.g. if you want to replace " " with "+", you can call:

    ```
    norm = normalizer(override={" ": "+"})
    norm("text (with spaces)")
    ```

    And you will get `"text+-with+spaces-"`.

    You can also remove a forbidden character altogether, by setting its value to
    `None` in the `override` argument. E.g if you want to remove parenthesis,
    you can call:

    ```
    norm = normalizer(override={"(": None, ")": None})
    norm("text (with parenthesis)")
    ```

    If you want to remove ALL special characters (except whitespace):

    ```
    norm = normalizer(replacement="", override={ " ": "-" })
    norm("text (with & special > characters)")
    ```

    And you will get `"text-with-special-characters"`

    The function does not trim the string size to 256 characters, because
    you might want the full normalized original string to store it somewhere
    else before truncating.
    """

    def __init__(self, replacement: str = "-", override: Optional[Mapping[str, str]] = None):
        """Set the default replacement string and custom override mapping"""
        if override is None:
            override = {}
        forbidden_chars = {
            "&": replacement,
            "?": replacement,
            "/": replacement,
            "#": replacement,
            "<": replacement,
            ">": replacement,
            '"': replacement,
            "'": replacement,
            "=": replacement,
            ";": replacement,
            "(": replacement,
            ")": replacement
        }
        source = []
        target = []
        remove = []
        for key, val in forbidden_chars.items():
            custom = override.get(key, val)
            if custom is None or custom == "":
                remove.append(key)
            else:
                if len(custom) > 1:
                    raise ValueError(f"wrong override '{custom}' for char '{key}': must be a single character")
                source.append(key)
                target.append(custom)
        self.space_replacement = override.get(" ", replacement) or ""
        self.table = str.maketrans(
            "".join(source), "".join(target), "".join(remove))

    def __call__(self, text: str) -> str:
        """Normalize text to NGSI entity ID"""
        global _whitespace_re
        ascii = unicodedata.normalize('NFD', text).encode('utf-8').decode('ascii', errors='ignore')
        without_control_chars = "".join(ch for ch in ascii if unicodedata.category(ch)[0] != "C")
        without_specials = without_control_chars.translate(self.table).strip()
        return _whitespace_re.sub(self.space_replacement, without_specials)
