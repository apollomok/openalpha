#!/usr/bin/env python3

import setuptools
import glob
import pyarrow as pa
import numpy as np 
arrow = pa.get_include()

setuptools.setup(
    name='openalpha',
    version='1.0',
    ext_modules=[
        setuptools.Extension(
            'openalpha',
            glob.glob('src/openalpha/*cc'),
            extra_compile_args=['-std=c++17', '-Wno-deprecated-declarations'],
            include_dirs=[
                './src',
                arrow,
                np.get_include(),
            ],
            libraries=[
                'boost_python3',
                'boost_numpy3',
                'boost_system',
                'boost_date_time',
                'boost_program_options',
                'boost_filesystem',
                'log4cxx',
                'arrow_python',
            ],
            library_dirs=[arrow + '/..'],
        ),
    ],
    include_package_data=True)
