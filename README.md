# OpenAlpha

OpenAlpha is an equity statistical arbitrage backtest simulator, use the same API as WorldQuant's [WebSim](https://www.worldquantvrc.com/en/cms/wqc/websim). It targets professional users, support both Python and C++.

## Installation Prerequisites

This installation is based on Ubuntu 18.04. We need build boost to support boost numpy of python3. If you have installed boost-dev from apt, please uninstall first.

```bash
sudo -i
apt-get install -y \
    g++  \
    make \
    cmake \
    clang-format \
    clang \
    python \
    python3-dev \
    vim \
    exuberant-ctags \
    git \
    wget \
    liblog4cxx-dev \
    python3-tk \
    libhdf5-dev \
  && pip3 install six numpy pandas cython pytest matplotlib tables h5py \
  && wget https://dl.bintray.com/boostorg/release/1.65.1/source/boost_1_65_1.tar.gz \
  && tar xzf boost_1_65_1.tar.gz \
  && cd boost_1_65_1 \
  && ./bootstrap.sh --prefix=/usr/local --with-python-version=3.6 \
  && ./b2 -j `getconf _NPROCESSORS_ONLN` && ./b2 install \
  && cd .. \
  && /bin/rm -rf boost*
  && exit
```

## Build OpenAlpha

```bash
cd ~
echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib" >> .bashrc
source .bashrc
git clone https://github.com/opentradesolutions/openalpha
cd openalpha
make
```

## Run sample alpha
It runs [sample.py](https://github.com/opentradesolutions/openalpha/blob/master/sample.py) and [sampe.cc](https://github.com/opentradesolutions/openalpha/blob/master/src/alpha/sample/sample.cc) configured in [openalpha.conf](https://github.com/opentradesolutions/openalpha/blob/master/openalpha.conf).
```bash
#download data first
wget -O data.tgz https://www.dropbox.com/s/wdernq2kz3rgcoo/openalpha.tar.xz?dl=0; tar xJf data.tgz
./scripts/data.py -a par2h5 data/symbol.par data/date.par data/adv60.par data/close.par data/industrygroup.par data/industry.par data/industry.par data/sector.par data/subindustry.par

./build/release/openalpha/openalpha
```

## Introduction to data

All data files are stored in hdf5 file format. Please have a look at [data files](https://www.dropbox.com/s/wdernq2kz3rgcoo/openalpha.tar.xz?dl=0). "data/symbol.h5" defines all instruments. "data/dates.h5" defines all dates. All the other files are 2D arrays. The row is indexed by date, we call it di in our code. The column is indexed by instrument, we call it ii in our code. The transposed version of parquet file is suffixed with '_t', e.g. transposed 'close.h5' file is named with 'close_t.h5'. There are some help functions in [scripts/data.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/data.py) for data handling.

## Report

Openalpha has default report, and dump out daily pnl file. You can also use [scripts/simsummary.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/simsummary.py) on the daily pnl file to generate more detailed report, plot, and do correlation calculation. Or you can use [ffn](http://pmorissette.github.io/ffn/).

## Python version OpenAlpha

[scripts/openalpha.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/openalpha.py) is a simplified pure-python version of openalpha. The performance can be optimized with cython. You can run it as below.

```bash
./scripts/openalpha.py -l 2 sample.py
```
