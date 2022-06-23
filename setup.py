#!/usr/bin/env python
# coding: utf8

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='airflow-provider-toloka',
    packages=['toloka_provider', *(f'toloka_provider.{package}' for package in find_packages('toloka_provider'))],
    version='0.0.6',
    description='A Toloka provider for Apache Airflow',
    long_description=readme,
    long_description_content_type='text/markdown',
    entry_points={
        'apache_airflow_provider': [
            'provider_info=toloka_provider.__init__:get_provider_info',
        ],
    },
    license='Apache License 2.0',
    author='Denis Makarov',
    author_email='pocoder@toloka.ai',
    python_requires='>=3.7.0',
    install_requires=[
        'apache-airflow>=2.1.0',
        'toloka-kit>=0.1.24',
    ],
    include_package_data=True,
    project_urls={
        'Source': 'https://github.com/Toloka/airflow-provider-toloka',
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Framework :: Apache Airflow',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
        'Typing :: Typed',
    ],
)
