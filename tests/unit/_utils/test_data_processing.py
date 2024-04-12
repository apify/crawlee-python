from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

import pytest

from crawlee._utils.data_processing import (
    filter_out_none_values_recursively,
    maybe_extract_enum_member_value,
    maybe_parse_body,
    maybe_parse_bool,
    maybe_parse_datetime,
    maybe_parse_float,
    maybe_parse_int,
    raise_on_duplicate_storage,
    raise_on_non_existing_storage,
)
from crawlee.storages.types import StorageTypes


@pytest.mark.parametrize(
    ('input_dict', 'expected_output', 'remove_empty_dicts'),
    [
        ({'key': None, 'key2': 'value'}, {'key2': 'value'}, False),
        ({'key': {'subkey': None}, 'key2': 'value'}, {'key': {}, 'key2': 'value'}, False),
        ({'key': {'subkey': None}, 'key2': 'value'}, {'key2': 'value'}, True),
        ({}, {}, False),
        ({'key': None}, {}, False),
        ({'key': None}, None, True),
        ({'key': {'subkey': None, 'sk2': 'value'}, 'k2': 'value'}, {'key': {'sk2': 'value'}, 'k2': 'value'}, False),
        ({'key': {'subkey': {'subsubkey': None}}, 'key2': 'value'}, {'key': {'subkey': {}}, 'key2': 'value'}, False),
        ({'key': {'subkey': {'subsubkey': None}}, 'key2': 'value'}, {'key2': 'value'}, True),
    ],
    ids=[
        'single_level_none',
        'nested_level_none',
        'remove_nested_empty_dict',
        'empty_dict',
        'all_none_values',
        'all_none_values_remove_empty',
        'mixed_nested',
        'deep_nested_none',
        'deep_nested_remove_empty',
    ],
)
def test_filter_out_none_values_recursively(
    input_dict: dict,
    expected_output: dict,
    *,
    remove_empty_dicts: bool,
) -> None:
    output = filter_out_none_values_recursively(input_dict, remove_empty_dicts=remove_empty_dicts)
    assert output == expected_output, f'Test failed: {output} != {expected_output}'


def test_maybe_extract_enum_member_value() -> None:
    class Color(Enum):
        RED = 1
        GREEN = 2
        BLUE = 3

    assert maybe_extract_enum_member_value(Color.RED) == 1
    assert maybe_extract_enum_member_value(Color.GREEN) == 2
    assert maybe_extract_enum_member_value(Color.BLUE) == 3
    assert maybe_extract_enum_member_value(10) == 10
    assert maybe_extract_enum_member_value('test') == 'test'
    assert maybe_extract_enum_member_value(None) is None


def test_maybe_parse_body() -> None:
    json_body = b'{"key": "value"}'
    xml_body = b'<note><to>Tove</to><from>Jani</from></note>'
    text_body = b'Plain text content'
    binary_body = b'\x00\x01\x02'

    assert maybe_parse_body(json_body, 'application/json') == {'key': 'value'}
    assert maybe_parse_body(xml_body, 'application/xml') == '<note><to>Tove</to><from>Jani</from></note>'
    assert maybe_parse_body(text_body, 'text/plain') == 'Plain text content'
    assert maybe_parse_body(binary_body, 'application/octet-stream') == binary_body
    assert maybe_parse_body(xml_body, 'text/xml') == '<note><to>Tove</to><from>Jani</from></note>'
    assert maybe_parse_body(text_body, 'text/plain; charset=utf-8') == 'Plain text content'


def test_maybe_parse_bool() -> None:
    assert maybe_parse_bool('True') is True
    assert maybe_parse_bool('true') is True
    assert maybe_parse_bool('1') is True
    assert maybe_parse_bool('False') is False
    assert maybe_parse_bool('false') is False
    assert maybe_parse_bool('0') is False
    assert maybe_parse_bool(None) is False
    assert maybe_parse_bool('bflmpsvz') is False


def test_maybe_parse_datetime() -> None:
    assert maybe_parse_datetime('2022-12-02T15:19:34.907Z') == datetime(
        2022, 12, 2, 15, 19, 34, 907000, tzinfo=timezone.utc
    )
    assert maybe_parse_datetime('2022-12-02T15:19:34.907') == '2022-12-02T15:19:34.907'
    assert maybe_parse_datetime('anything') == 'anything'


def test_maybe_parse_float() -> None:
    assert maybe_parse_float('0') == 0.0
    assert maybe_parse_float('1') == 1.0
    assert maybe_parse_float('-1') == -1.0
    assert maybe_parse_float('3.14159') == 3.14159
    assert maybe_parse_float('-123.456') == -123.456
    assert maybe_parse_float('1e3') == 1000.0
    assert maybe_parse_float('2.5e-3') == 0.0025
    assert maybe_parse_float('') is None
    assert maybe_parse_float('abcd') is None
    assert maybe_parse_float('1.2.3') is None
    assert maybe_parse_float('infinity') == float('inf')
    assert maybe_parse_float('-infinity') == float('-inf')


def test_maybe_parse_int() -> None:
    assert maybe_parse_int('0') == 0
    assert maybe_parse_int('1') == 1
    assert maybe_parse_int('-1') == -1
    assert maybe_parse_int('136749825') == 136749825
    assert maybe_parse_int('') is None
    assert maybe_parse_int('abcd') is None


def test_raise_on_duplicate_storage() -> None:
    with pytest.raises(ValueError, match='Dataset with name "test" already exists.'):
        raise_on_duplicate_storage(StorageTypes.DATASET, 'name', 'test')


def test_raise_on_non_existing_storage() -> None:
    with pytest.raises(ValueError, match='Dataset with id "kckxQw6j6AtrgyA09" does not exist.'):
        raise_on_non_existing_storage(StorageTypes.DATASET, 'kckxQw6j6AtrgyA09')
