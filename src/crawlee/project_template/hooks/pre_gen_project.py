# % if cookiecutter.package_manager == 'poetry'
import subprocess
import re

try:
    version = subprocess.check_output(['poetry', '--version']).decode().strip()
except OSError as exc:
    raise RuntimeError('You chose to use the Poetry package manager, but it does not seem to be installed') from exc

if not re.match(r'Poetry \(version 1\..*\)', version):
    raise RuntimeError(f'Poetry 1.x is required, but "{version}" is installed')
# % endif
