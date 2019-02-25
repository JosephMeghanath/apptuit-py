"""
utilises for apptuit
"""
import os
from string import ascii_letters, digits
import warnings

from apptuit import APPTUIT_PY_TAGS, DEPRECATED_APPTUIT_PY_TAGS

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

VALID_CHARSET = set(ascii_letters + digits + "-_./")
INVALID_CHARSET = frozenset(map(chr, range(128))) - VALID_CHARSET


@lru_cache(maxsize=None)
def _contains_valid_chars(string):
    return INVALID_CHARSET.isdisjoint(string)


def _validate_tags(tags):
    for tagk in tags.keys():
        if not tagk or not _contains_valid_chars(tagk):
            raise ValueError("Tag key %s contains an invalid character, "
                             "allowed characters are a-z, A-Z, 0-9, -, _, ., and /" % tagk)


def _get_tags_from_environment():
    tags_str = os.environ.get(APPTUIT_PY_TAGS)
    if not tags_str:
        tags_str = os.environ.get(DEPRECATED_APPTUIT_PY_TAGS)
        if tags_str:
            warnings.warn("The environment variable %s is deprecated, please use %s instead"
                          % (DEPRECATED_APPTUIT_PY_TAGS, APPTUIT_PY_TAGS), DeprecationWarning)
    if not tags_str:
        return {}
    tags = {}
    tags_str = tags_str.strip(", ")
    tags_split = tags_str.split(',')
    for tag in tags_split:
        tag = tag.strip()
        if not tag:
            continue
        try:
            key, val = tag.split(":")
            tags[key.strip()] = val.strip()
        except ValueError:
            raise ValueError("Invalid format for "
                             + APPTUIT_PY_TAGS +
                             ", failed to parse tag key-value pair '"
                             + tag + "', " + APPTUIT_PY_TAGS + " should be in the format - "
                                                               "'tag_key1:tag_val1,tag_key2:tag_val2,...,tag_keyN:tag_valN'")
    _validate_tags(tags)
    return tags
