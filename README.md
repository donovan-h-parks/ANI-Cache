# ANI-Cache
[![License](https://img.shields.io/github/license/dparks1134/ANI-Cache)](https://img.shields.io/github/license/dparks1134/ANI-Cache)
[![version status](https://img.shields.io/pypi/v/ani-cache.svg)](https://pypi.python.org/pypi/ani-cache)
[![Actions Status](https://github.com/dparks1134/ANI-Cache/workflows/pytesting/badge.svg)](https://github.com/dparks1134/ANI-Cache/workflows/pytesting/actions)
[![Github All Releases](https://img.shields.io/github/downloads/dparks1134/ANI-Cache/total.svg)]()
[![Bioconda](https://img.shields.io/conda/vn/bioconda/ani-cache.svg?color=43b02a)](https://anaconda.org/bioconda/cache)


ANI-Cache provides a wrapper around [FastANI](https://github.com/ParBLiSS/FastANI) which stores calculated ANI values to a SQLite database. This permits fast lookup of previously calculated values and sharing of precomputed ANI databases between researchers.  

## Installation

### Install via Conda

[IN PROGRESS] ANI-Cache can be install via Conda using:
```
>conda install -c bioconda ani_cache
```

### Install via pip

ANI-Cache can be installed using [pip](https://pypi.org/project/ani-cache/) using:
```
> pip install ani-cache
```
You must install [FastANI](https://github.com/ParBLiSS/FastANI) independently.

### Dependencies

ANI-Cache assumes the following 3rd party dependencies are on your system path:
* [FastANI](https://github.com/ParBLiSS/FastANI) >= 1.0: Jain et al., 2018. High throughput ANI analysis of 90K prokaryotic genomes reveals clear species boundaries. <i>Nature Communications</i>, <b>9</b>: 5114.

## Quick Start

The functionality provided by ANI-Cache can be accessed through the help menu:
```
> ani-cache -h
```

Usage information about specific functions can also be accessed through the help menu, e.g.:
```
> ani-cache fastani –h
```

## Cite

Please cite the [FastANI](https://github.com/ParBLiSS/FastANI) manuscript and this repository (https://github.com/dparks1134/ANI-Cache).


## Copyright

Copyright © 2021 Donovan Parks. See LICENSE for further details.
