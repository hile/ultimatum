#!/usr/bin/env python
"""
Scripts and system tool wrappers for FreeBSD (and other *BSDs)

This module is split from module to platform dependent tool
"""

import sys,os,glob
from setuptools import setup

VERSION='3.0.0'
README = open(os.path.join(os.path.dirname(__file__),'README.md'),'r').read()

setup(
    name = 'ultimatum',
    keywords = 'System Management Utility FreeBSD Scripts',
    description = 'Sysadmin utility modules and scripts for FreeBSD',
    author = 'Ilkka Tuohela', 
    author_email = 'hile@iki.fi',
    long_description = README, 
    version = VERSION,
    url = 'http://tuohela.net/packages/ultimatum',
    license = 'PSF',
    zip_safe = False,
    packages = ['ultimatum'],
    scripts = glob.glob('bin/*'),
    install_requires = [ 'systematic>=3.0.0' ],
)

