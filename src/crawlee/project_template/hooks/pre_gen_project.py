# % if cookiecutter.package_manager in ['poetry', 'uv']
import subprocess
import shutil
import re
import sys

manager = "{{cookiecutter.package_manager}}"
manager_text = manager.title()
# % if cookiecutter.package_manager == 'poetry'
version_regex = r'Poetry \(version 2\..*\)'
r_version = '2.x'
# % elif cookiecutter.package_manager == 'uv'
version_regex = r'uv (0\..*)'
r_version = '0.x'
# % endif

# Check if package manager is available in PATH
if not shutil.which(manager):
    sys.stderr.write(f'\nError: You selected {manager_text} as your package manager, but it is not installed. Please install it and try again.\n')
    sys.exit(1)

# Check if the package manager is executable
try:
    version = subprocess.check_output([manager, '--version']).decode().strip()
except OSError:
    sys.stderr.write(f'\nError: Your selected package manager {manager_text} was found but failed to execute.\n')
    sys.exit(1)

# Check if the version matches the required regex
if not re.match(version_regex, version):
    sys.stderr.write(f'\nError: Your selected package manager {manager_text} requires version {r_version}, but {version} is installed.\n')
    sys.exit(1)
# % endif
