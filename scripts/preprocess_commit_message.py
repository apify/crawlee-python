from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

pr_issues_file = Path.cwd() / 'pullRequestIssues.json'


def load_pr_issues() -> dict[int, list[int]]:
    if pr_issues_file.exists():
        return {int(key): value for key, value in json.load(pr_issues_file.open('r')).items()}

    return {}


def issue_link(issue_number: int) -> str:
    return f'[#{issue_number}](<REPO>/issues/{issue_number})'


def pr_link(pr_number: int) -> str:
    return f'[#{pr_number}](<REPO>/pull/{pr_number})'


def replace_issue_or_pull_request_number(match: re.Match) -> str:
    item_number = int(match.group(2))

    pr_to_issues = load_pr_issues()

    if item_number not in pr_to_issues:
        subprocess.check_call(str(Path(__file__).parent / 'fetch_pr_issues.sh'))  # noqa: S603
        pr_to_issues = load_pr_issues()

    issue_links = [issue_link(issue_number) for issue_number in pr_to_issues.get(item_number, [])]

    if item_number not in pr_to_issues:
        return f'({issue_link(item_number)})'

    if not issue_links:
        return f'({pr_link(item_number)})'

    return f'({pr_link(item_number)}, closes {", ".join(issue_links)})'


if __name__ == '__main__':
    print(
        re.sub(
            r'\((\w+\s)?#([0-9]+)\)',
            repl=replace_issue_or_pull_request_number,
            string=sys.stdin.read(),
        )
    )
