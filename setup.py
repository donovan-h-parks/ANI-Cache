import os

from setuptools import setup


def version():
    """Get version from VERSION file."""

    setup_dir = os.path.dirname(os.path.realpath(__file__))
    version_file = open(os.path.join(setup_dir, 'ani_cache', 'VERSION'))
    return version_file.readline().strip()


setup(
    name='ani_cache',
    version=version(),
    author='Donovan Parks',
    author_email='donovan.parks@gmail.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
    data_files=[("", ["LICENSE"])],
    packages=['ani_cache', 'ani_cache.tests'],
    scripts=['bin/ani_cache'],
    package_data={'ani_cache': ['VERSION']},
    url='http://pypi.python.org/pypi/ani_cache/',
    license='GPL3',
    description='Cache-aware ANI calculations.',
    install_requires=[],
)
