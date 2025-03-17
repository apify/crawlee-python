from __future__ import annotations

from crawlee._utils.console import make_table


def test_empty_input() -> None:
    assert make_table([]) == ''


def test_empty_row() -> None:
    assert make_table([()]) == ''


def test_single_column() -> None:
    result = make_table([('test',)])
    lines = result.split('\n')
    assert len(lines) == 3
    assert lines[1] == '│ test │'


def test_two_columns() -> None:
    data = [('Name', 'Age'), ('Alice', '30'), ('Bob', '25')]
    result = make_table(data)
    lines = result.split('\n')
    # fmt: off
    assert lines == ['┌───────┬─────┐',
                     '│ Name  │ Age │',
                     '│ Alice │ 30  │',
                     '│ Bob   │ 25  │',
                     '└───────┴─────┘']
    # fmt: on


def test_long_content_truncation() -> None:
    data = [('Short', 'VeryVeryVeryLongContent')]
    result = make_table(data, width=25)
    lines = result.split('\n')
    # fmt: off
    assert lines == ['┌───────────┬───────────┐',
                     '│ Short     │ VeryVe... │',
                     '└───────────┴───────────┘']
    # fmt: on
