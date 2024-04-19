from __future__ import annotations

import fnmatch
import os
import re
from typing import Sequence


class Glob:
    """Wraps a glob pattern (supports the `*`, `**`, `?` wildcards)."""

    def __init__(self, glob: str) -> None:
        self.glob = glob
        self.regexp = re.compile(_translate(self.glob))


def _translate(
    pat: str, *, recursive: bool = False, include_hidden: bool = False, seps: Sequence[str] | None = None
) -> str:
    """Translate a pathname with shell wildcards to a regular expression.

    If `recursive` is true, the pattern segment '**' will match any number of
    path segments.

    If `include_hidden` is true, wildcards can match path segments beginning
    with a dot ('.').

    If a sequence of separator characters is given to `seps`, they will be
    used to split the pattern into segments and match path separators. If not
    given, os.path.sep and os.path.altsep (where available) are used.

    HACK: This function is copied from CPython stdlib source. It will be released in Python 3.13 as `glob.translate`
    """
    if not seps:
        seps = (os.path.sep, os.path.altsep) if os.path.altsep else os.path.sep

    escaped_seps = ''.join(map(re.escape, seps))
    any_sep = f'[{escaped_seps}]' if len(seps) > 1 else escaped_seps
    not_sep = f'[^{escaped_seps}]'
    if include_hidden:
        one_last_segment = f'{not_sep}+'
        one_segment = f'{one_last_segment}{any_sep}'
        any_segments = f'(?:.+{any_sep})?'
        any_last_segments = '.*'
    else:
        one_last_segment = f'[^{escaped_seps}.]{not_sep}*'
        one_segment = f'{one_last_segment}{any_sep}'
        any_segments = f'(?:{one_segment})*'
        any_last_segments = f'{any_segments}(?:{one_last_segment})?'

    results = []
    parts = re.split(any_sep, pat)
    last_part_idx = len(parts) - 1
    for idx, part in enumerate(parts):
        if part == '*':
            results.append(one_segment if idx < last_part_idx else one_last_segment)
        elif recursive and part == '**':
            if idx < last_part_idx:
                if parts[idx + 1] != '**':
                    results.append(any_segments)
            else:
                results.append(any_last_segments)
        else:
            if part:
                if not include_hidden and part[0] in '*?':
                    results.append(r'(?!\.)')
                results.extend(fnmatch._translate(part, f'{not_sep}*', not_sep))  # type: ignore  # noqa: SLF001
            if idx < last_part_idx:
                results.append(any_sep)
    res = ''.join(results)
    return rf'(?s:{res})\Z'
