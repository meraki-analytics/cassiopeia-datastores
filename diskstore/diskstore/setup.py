#!/usr/bin/env python

import sys

from setuptools import setup, find_packages


install_requires = [
    "cassiopeia",
    "datapipelines>=1.0.3",
    "simplekv"
]

# Require python 3.6
if sys.version_info.major != 3 and sys.version_info.minor != 6:
    sys.exit("Cassiopeia requires Python 3.6.")

setup(
    name="cassiopeia-diskstore",
    version="1.0.1",
    author="Jason Maldonis; Rob Rua",
    author_email="team@merakianalytics.com",
    url="https://github.com/meraki-analytics/cassiopeia-datastores/tree/master/diskstore",
    description="A disk data store for the Cassiopeia League of Legends wrapper.",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Environment :: Web Environment",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License"
    ],
    license="MIT",
    packages=find_packages(),
    zip_safe=True,
    install_requires=install_requires,
    include_package_data=True
)
