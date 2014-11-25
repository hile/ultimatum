#!/usr/bin/env python
"""
Scripts and system tool wrappers for FreeBSD (and other *BSDs)

This module is split from module to platform dependent tool
"""

import sys
import os
import glob

from setuptools import setup, find_packages

VERSION='4.0.6'

setup(
    name = 'ultimatum',
    keywords = 'System Management Utility FreeBSD Scripts',
    description = 'Sysadmin utility modules and scripts for FreeBSD',
    author = 'Ilkka Tuohela',
    author_email = 'hile@iki.fi',
    version = VERSION,
    url = 'http://tuohela.net/packages/ultimatum',
    license = 'PSF',
    zip_safe = False,
    packages = find_packages(),
    scripts = glob.glob('bin/*'),
    install_requires = (
        'seine>=2.4.1',
        'systematic>=4.2.0',
    ),
)

