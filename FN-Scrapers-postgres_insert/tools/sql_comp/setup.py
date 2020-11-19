import os
from setuptools import setup, find_packages


def read(file_name):
    with open(os.path.join(os.path.dirname(__file__), file_name)) as f:
        return f.read()

setup(
    name='sql_comp',
    version='0.0.1',
    packages=find_packages(),
    description='SQL Compare',
    long_description=read('README.md')
)

