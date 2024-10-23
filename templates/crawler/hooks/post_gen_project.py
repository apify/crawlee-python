import subprocess

# % if cookiecutter.package_manager == 'poetry'
subprocess.check_call(['poetry', 'install'])
# % if cookiecutter.crawler_type == 'playwright'
subprocess.check_call(['poetry', 'run', 'playwright', 'install'])
# % endif
# % elif cookiecutter.package_manager == 'pip'
subprocess.check_call(['pip', 'install', '.'])
# % if cookiecutter.crawler_type == 'playwright'
subprocess.check_call(['playwright', 'install'])
# % endif
# % endif
