#!/usr/bin/env python
# coding: utf8

from setuptools import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='toloka-airflow',
    packages=['toloka_airflow'],
    version='0.0.2',
    description='Toloka airflow library',
    long_description=readme,
    long_description_content_type='text/markdown',
    license='Apache 2.0',
    author='Denis Makarov',
    author_email='pocoder@yandex-team.ru',
    python_requires='>=3.7.0',
    install_requires=[
        'apache-airflow',
        'toloka-kit>=0.1.21',
        'pandas>=1.1.0',
        'apache-airflow-providers-amazon',
    ],
    include_package_data=True,
    project_urls={
        'Source': 'https://github.com/Toloka/toloka-airflow',
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
        'Typing :: Typed',
    ],
)
