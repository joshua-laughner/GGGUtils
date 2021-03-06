from setuptools import setup, find_packages

setup(
    name='GGGUtils',
    version='0.1',
    packages=find_packages(),
    url='https://github.com/joshua-laughner/GGGUtils.git',
    license='',
    author='Joshua Laughner',
    author_email='jlaugh@caltech.edu',
    install_requires=['textui', 'configobj'],
    description='Ancilliary utilities to run GGG',
    entry_points={
        'console_scripts': ['gggutils=gggutils.console_main:main']
    }
)
