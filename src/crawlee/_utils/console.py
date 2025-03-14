from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

BORDER = {'TL': '┌', 'TR': '┐', 'BL': '└', 'BR': '┘', 'H': '─', 'V': '│', 'TM': '┬', 'BM': '┴'}


def make_table(rows: Sequence[Sequence[str]], width: int = 100) -> str:
    """Creates a text table using Unicode characters.

    Args:
        rows: A list of tuples/lists to be displayed in the table.
        width: Maximum width of the table.
    """
    if not rows:
        return ''

    num_cols = max(len(row) for row in rows)

    if num_cols == 0:
        return ''

    # Normalize the row size by filling missing columns with empty values
    normalized_rows = [list(row) + [''] * (num_cols - len(row)) for row in rows]
    col_widths = [max(len(str(row[i])) for row in normalized_rows) for i in range(num_cols)]
    total_width = sum(col_widths) + (3 * num_cols) + 1

    # If the table size is larger than `width`, set all columns to the same length
    col_widths = col_widths if total_width <= width else [max(3, (width - (3 * num_cols) - 1) // num_cols)] * num_cols

    # Initialize borders
    top_parts, bottom_parts = [BORDER['TL']], [BORDER['BL']]

    for i in range(num_cols):
        h_border = BORDER['H'] * (col_widths[i] + 2)
        top_parts.append(h_border)
        bottom_parts.append(h_border)

        if i < num_cols - 1:
            top_parts.append(BORDER['TM'])
            bottom_parts.append(BORDER['BM'])
        else:
            top_parts.append(BORDER['TR'])
            bottom_parts.append(BORDER['BR'])

    top_border, bottom_border = ''.join(top_parts), ''.join(bottom_parts)

    result = [top_border]

    for row in normalized_rows:
        cells = []

        for i, cell in enumerate(row):
            # Trim the content if the length exceeds the widths of the column
            norm_cell = f'{cell[: col_widths[i] - 3]}...' if len(cell) > col_widths[i] else cell.ljust(col_widths[i])
            cells.append(norm_cell)

        # row: │ cell1 │ cell2 │ ...
        row_str = BORDER['V'] + ''.join(f' {cell} {BORDER["V"]}' for cell in cells)
        result.append(row_str)

    result.append(bottom_border)

    return '\n'.join(result)
