from __future__ import annotations

from enum import Enum

import pytest

from crawlee._utils.data_processing import (
    filter_out_none_values_recursively,
    maybe_extract_enum_member_value,
    maybe_parse_body,
    raise_on_duplicate_storage,
    raise_on_non_existing_storage,
)
from crawlee.types import StorageTypes


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


def test_raise_on_duplicate_storage() -> None:
    with pytest.raises(ValueError, match='Dataset with name "test" already exists.'):
        raise_on_duplicate_storage(StorageTypes.DATASET, 'name', 'test')


def test_raise_on_non_existing_storage() -> None:
    with pytest.raises(ValueError, match='Dataset with id "kckxQw6j6AtrgyA09" does not exist.'):
        raise_on_non_existing_storage(StorageTypes.DATASET, 'kckxQw6j6AtrgyA09')
