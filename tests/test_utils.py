"""
Test for functions defined in the utils module
"""
from nose.tools import assert_true, assert_false, assert_raises, \
    assert_equals

from apptuit.utils import strtobool, sanitize_name_apptuit, \
    sanitize_name_prometheus


def test_strtobool():
    """
    Test strtobool
    """
    true_values = ('y', 'yes', 't', 'true', 'on', '1')
    false_values = ('n', 'no', 'f', 'false', 'off', '0')
    other_values = ('truee', 'ffalse', 'nno', '01')
    for val in true_values:
        assert_true(strtobool(val))
    for val in false_values:
        assert_false(strtobool(val))
    for val in other_values:
        with assert_raises(ValueError):
            strtobool(val)


def test_sanitize_apptuit():
    """
    Test that sanitize name for apptuit works
    """
    test_names = {
        "metric_name tag-key.str": "metric_name_tag-key.str",
        u"&*)": "___",
        "": "",
        "abc.abc-abc/abc_abc": "abc.abc-abc/abc_abc",
        " ": "_",
        u'日本語.abc': "日本語.abc",
        u'abc.日本語': "abc.日本語"

    }
    for test_name, expected_name in test_names.items():
        result = sanitize_name_apptuit(test_name)
        assert_equals(result, expected_name, "Validation failed for," + test_name)


def test_sanitize_prometheus():
    """
    Test that sanitize name for prometheus works
    """
    test_names = {
        "metric_name tag-key.str": "metric_name_tag_key_str",
        u"&*)": "_",
        "abc.abc-abc/abc_abc": "abc_abc_abc_abc_abc",
        " ": "_",
        u'日本語.abc': "_abc",
        u'abc.日本語': "abc____"
    }
    for test_name, expected_name in test_names.items():
        result = sanitize_name_prometheus(test_name)
        assert_equals(result, expected_name, "Validation failed for,'" + test_name+"'")
