# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE.md') as f:
    license = f.read()

setup(
    name='spot',
    version='0.0.1',
    description='Automated setter for Spark job parameters based on regression of previous runs in Spark history',
    long_description=readme,
    author='Dzmitry Makatun',
    author_email='dzmitry.makatun@absa.africa',
    url='https://github.com/AbsaOSS',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)