# % if cookiecutter.package_manager in ['poetry', 'uv']
import subprocess
import re

manager = "{{cookiecutter.package_manager}}"
manager_text = manager.title()
# % if cookiecutter.package_manager == 'poetry'
version_regex = r'Poetry \(version 2\..*\)'
r_version = '2.x'
# % elif cookiecutter.package_manager == 'uv'
version_regex = r'uv (0\..*)'
r_version = '0.x'
# % endif

try:
    version = subprocess.check_output([manager, '--version']).decode().strip()
except OSError as exc:
    raise RuntimeError(f'You chose to use the {manager_text} package manager, but it does not seem to be installed') from exc
if not re.match(version_regex, version):
    raise RuntimeError(f'{manager_text} {r_version} is required, but "{version}" is installed')
# % endif
