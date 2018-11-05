from __future__ import absolute_import
from __future__ import print_function


import subprocess
from distutils.command.build import build as _build

import setuptools


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name

    sub_commands = _build.sub_commands + [('CustomCommands', None)]

CUSTOM_COMMANDS = [
    ['apt-get', 'update'],
    ['apt-get', '-y', 'install', 'python-dev'],
    ['apt-get', '-y', 'install', 'libffi-dev'],
    ['apt-get', '-y', 'install', 'libssl-dev'],
    ['pip', 'install', 'cryptography'],
    ['pip', 'install', 'oauth2client'],
]


class CustomCommands(setuptools.Command):
  """A setuptools Command class able to run arbitrary commands."""

  def initialize_options(self):
    pass

  def finalize_options(self):
    pass

  def RunCustomCommand(self, command_list):
    print('Running command: %s' % command_list)
    p = subprocess.Popen(
        command_list,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # Can use communicate(input='y\n'.encode()) if the command run requires
    # some confirmation.
    stdout_data, _ = p.communicate()
    print('Command output: %s' % stdout_data)
    if p.returncode != 0:
      raise RuntimeError(
          'Command %s failed: exit code: %s' % (command_list, p.returncode))

  def run(self):
    for command in CUSTOM_COMMANDS:
      self.RunCustomCommand(command)


REQUIRED_PACKAGES = [
    'canonicaljson',
    'httplib2',
    'oauth2client',
    'google-api-python-client',
    'requests',
    'google-auth-httplib2',
    'expiringdict',
    'fluent-logger'    
]

setuptools.setup(
    name='gcp_encryption',
    version='0.0.1',
    description='My primary codebase.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)