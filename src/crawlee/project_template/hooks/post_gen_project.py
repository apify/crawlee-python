import platform
import subprocess
from pathlib import Path

Path('_pyproject.toml').rename('pyproject.toml')

# % if cookiecutter.package_manager == 'poetry'
Path('requirements.txt').unlink()

subprocess.check_call(['poetry', 'install'])
# % if cookiecutter.crawler_type == 'playwright'
subprocess.check_call(['poetry', 'run', 'playwright', 'install'])
# % endif
# % elif cookiecutter.package_manager == 'pip'
import venv  # noqa: E402

# Create a virtual environment
venv_root = Path('.venv')
venv.main([str(venv_root)])

if platform.system() == 'Windows':  # noqa: SIM108
    path = venv_root / 'Scripts'
else:
    path = venv_root / 'bin'

# Install requirements and generate requirements.txt as an impromptu lockfile
subprocess.check_call([str(path / 'pip'), 'install', '-r', 'requirements.txt'])
with open('requirements.txt', 'w') as requirements_txt:
    subprocess.check_call([str(path / 'pip'), 'freeze'], stdout=requirements_txt)

# % if cookiecutter.crawler_type == 'playwright'
subprocess.check_call([str(path / 'playwright'), 'install'])
# % endif
# % endif
