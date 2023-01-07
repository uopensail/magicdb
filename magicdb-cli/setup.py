#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: timepi
@description: this is setup script for magicdb-cli
"""

from setuptools import setup, find_packages

setup(
    name="magicdb",
    version="1.0.0",
    description="magicdb client cmd tool",
    license="License :: GPL 3",
    author="TimePi",
    author_email="timepi@uopensail.com",
    url="https://github.com/uopensail/magicdb",
    py_modules=["magicdb"],
    keywords="magicdb client",
    long_description="",
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=[
        "mmh3 == 3.0.0",
        "numpy == 1.19.5",
        "pandas == 1.1.5",
        "pyarrow == 6.0.1",
        "antlr4-python3-runtime == 4.10",
        "etcd3 == 0.12.0",
        "awswrangler == 2.18.0",
        "boto3 == 1.26.27",
    ],

    scripts=[],
    entry_points={
        'console_scripts': [
            'magicdb-cli = magicdb:main'
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: GPL 3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
    zip_safe=False
)