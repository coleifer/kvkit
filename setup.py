import os
from setuptools import setup, find_packages


setup(
    name='kvkit',
    version=__import__('kvkit').__version__,
    description='high-level python toolkit for ordered key/value stores',
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/kvkit/',
    packages=find_packages(),
    package_data = {
        'kvkit': [
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='runtests.runtests',
)
