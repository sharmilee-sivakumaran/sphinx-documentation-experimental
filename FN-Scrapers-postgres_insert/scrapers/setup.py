import os
from setuptools import setup, find_packages


def read(file_name):
    with open(os.path.join(os.path.dirname(__file__), file_name)) as f:
        return f.read()


install_requires = [
    "fn-service",
    "injector",
]

setup(
    name='fn-scrapers',
    version='0.0.1',
    package_data={'fn_scrapers': [
        'datatypes/*/*/schemas/*',
        'datatypes/events/common/metadata_mapping.json',
        'common/kraken/schemas/*/feeds/*',
    ]},
    install_requires=install_requires,
    packages=find_packages(),
    description='Scraper Code',
    long_description=read('README.md')
)

