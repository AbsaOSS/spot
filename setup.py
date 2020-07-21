# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE.md') as f:
    license = f.read()

setup(
    name='spot',
    version='0.0.1',
    description='A set of tools for monitoring and tuning of Spark app performance'
                ' based on statistical analysis of run history',
    long_description=readme,
    author='Dzmitry Makatun',
    author_email='dzmitry.makatun@absa.africa',
    url='https://github.com/AbsaOSS/spot',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)
