#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tornavro",
    version="0.0.1",
    description="Avro RPC using Tornado's IOLoop.",
    author="Rich Schumacher",
    author_email="rich.schu@gmail.com",
    url="https://github.com/richid/tornavro",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "tornado==2.2.0",
        "avro==1.6.2"
    ],
    keywords=["tornado", "avro"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ]
)
