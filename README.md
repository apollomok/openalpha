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
    libjemalloc-dev bison flex zlib1g-dev libsnappy-dev liblz4-dev libbrotli-dev libzstd-dev rapidjson-dev thrift-compiler autoconf \
  && pip3 install six numpy pandas cython pytest matplotlib \
  && wget https://dl.bintray.com/boostorg/release/1.65.1/source/boost_1_65_1.tar.gz \
  && tar xzf boost_1_65_1.tar.gz \
  && cd boost_1_65_1 \
  && ./bootstrap.sh --prefix=/usr/local --with-python-version=3.6 \
  && ./b2 -j `getconf _NPROCESSORS_ONLN` && ./b2 install \
  && cd .. \
  && /bin/rm -rf boost* \
  && git clone https://github.com/apache/arrow.git \
  && cd arrow/cpp \
  && mkdir -p release \
  && cd release \
  && cmake -DCMAKE_BUILD_TYPE=release -DCMAKE_INSTALL_PREFIX=/usr/local -DARROW_PARQUET=on -DARROW_PYTHON=on -DARROW_PLASMA=on -DARROW_BUILD_TESTS=OFF -DPYTHON_EXECUTABLE:FILEPATH=`which python3` -DPYTHON_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython3.6m.so.1.0 .. \
  && make -j `getconf _NPROCESSORS_ONLN` && make install \
  && cd ../../python \
  && python3 setup.py build_ext --inplace --with-parquet \
  && cp -rf pyarrow `python3 -c "import site; print(site.getsitepackages()[0])"` \
  && cd ../.. \
  && /bin/rm -rf arrow
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

./build/release/openalpha/openalpha
```

## Introduction to data

All data files are stored in parquet file format. Please have a look at [data files](https://www.dropbox.com/s/wdernq2kz3rgcoo/openalpha.tar.xz?dl=0). "data/symbol.par" defines all instruments. "data/dates.par" defines all dates. All the other files are 2D arrays. The row is indexed by date, we call it di in our code. The column is indexed by instrument, we call it ii in our code. The transposed version of parquet file is suffixed with '_t', e.g. transposed 'close.par' file is named with 'close_t.par'. There are some help functions in [scripts/data.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/data.py) for data handling.

## HFD5 format data file

In most of single-threaded cases, HFD5 data format should be faster than parquet. Please check out HFD5 branch.

https://github.com/opentradesolutions/openalpha/tree/hdf5

## Report

Openalpha has default report, and dump out daily pnl file. You can also use [scripts/simsummary.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/simsummary.py) on the daily pnl file to generate more detailed report, plot, and do correlation calculation. Or you can use [ffn](http://pmorissette.github.io/ffn/).

## Python version OpenAlpha

[scripts/openalpha.py](https://github.com/opentradesolutions/openalpha/blob/master/scripts/openalpha.py) is a simplified pure-python version of openalpha. The performance can be optimized with cython. You can run it as below.

```bash
./scripts/openalpha.py -l 2 sample.py
```
