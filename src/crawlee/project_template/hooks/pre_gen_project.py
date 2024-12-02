# % if cookiecutter.package_manager == 'poetry'
import subprocess

try:
    subprocess.check_call(['poetry', '--version'])
except OSError as exc:
    raise RuntimeError('You chose to use the Poetry package manager, but it does not seem to be installed') from exc
# % endif
