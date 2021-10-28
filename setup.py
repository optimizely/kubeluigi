import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        sys.exit(pytest.main(self.test_args))


version = '1.0.0'

setup(name='kubeluigi',
      version=version,
      description="Luigi contribution to run Tasks as Kubernetes Jobs",
      long_description=open("README.md").read(),
      classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Topic :: Software Development',
        'Environment :: Plugins',
      ],
      keywords='Kubeluigi Kubernetes luigi pipelines pipeline kubetask',
      author='Optimizely Datascience',
      author_email='david.przybilla@optimizely.com',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      tests_require=['pytest', 'mock'],
      cmdclass={'test': PyTest},
      install_requires=['kubernetes>=17.17.0', 'luigi'],
      entry_points={}
)
