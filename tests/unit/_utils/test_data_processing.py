import pytest

from crawlee._utils.data_processing import filter_out_none_values_recursively


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
