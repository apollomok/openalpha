#!/usr/bin/env python3

import pyarrow.parquet as pp
import sys
import numpy as np
import pyarrow as pa
import pandas as pd


def main():
  fn = sys.argv[1]
  write_array(
      fn.replace('.par', '_t.par'), np.transpose(pd.read_parquet(fn).values))


def write_array(fn, array):
  table = pa.Table.from_pandas(pd.DataFrame(array))
  writer = pp.ParquetWriter(fn, table.schema, compression='snappy')
  writer.write_table(table)
  writer.close()


main()
