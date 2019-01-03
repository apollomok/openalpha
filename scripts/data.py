#!/usr/bin/env python3

import pyarrow.parquet as pp
import sys
import numpy as np
import pyarrow as pa
import pandas as pd
from optparse import OptionParser
import os


def main():
  actions = ['ffill', 'validate', 'transpose']
  parser = OptionParser(
      usage='usage: %prog [options] filename', version='%prog 1.0')
  parser.add_option(
      '-a',
      '--action',
      type='choice',
      choices=actions,
      help='action, choose from [' + ', '.join(actions) + ']')
  (options, args) = parser.parse_args()
  action = options.action
  for fn in args or []:
    if action == 'ffill': ffill_file(fn)
    elif action == 'validate': validate(fn)
    elif action == 'transpose': transpose_file(fn)


def transpose_file(fn):
  write_array(
      fn.replace('.par', '_t.par'), np.transpose(pd.read_parquet(fn).values))


def ffill_file(fn):
  write_array(fn, ffill(pd.read_parquet(fn).values))


def validate(dir):
  arr = pd.read_parquet(os.path.join(dir, 'close.par')).values
  arr0 = np.copy(arr)
  arr = ffill(arr)
  out = np.argwhere(arr0 != arr)
  if len(out):
    print('close.par is not forward filled')
    print(len(out), ',', len(np.unique(out[:, 0])), '/', arr.shape[0], ',',
          len(np.unique(out[:, 1])), '/', arr.shape[1])
    print(out)
    print()
  diff = []
  for ii in range(arr.shape[1]):
    col = arr[:, ii]
    for j in range(1, len(col)):
      px0 = col[j - 1]
      px = col[j]
      if not (px0 > 0): continue
      x = px / px0
      if x > 2 or x < 0.5:
        diff.append((ii, j, int(100 * x)))
  if diff:
    diff = np.array(diff)
    print('big price change % in close.par')
    print(len(diff), ',', len(np.unique(diff[:, 0])), '/', arr.shape[0], ',',
          len(np.unique(diff[:, 1])), '/', arr.shape[1])
    print(np.array(diff))
    print()
  for name in ('sector', 'industry', 'industrygroup', 'subindustry'):
    arr = pd.read_parquet(os.path.join(dir, name + '.par')).values
    out = np.argwhere((arr <= 0) + np.isnan(arr))
    if len(out):
      print(name + '.par has invalid value')
      print(len(out), ',', len(np.unique(out[:, 0])), '/', arr.shape[0], ',',
            len(np.unique(out[:, 1])), '/', arr.shape[1])
      print(out)
      print()


def write_array(fn, array):
  table = pa.Table.from_pandas(pd.DataFrame(array))
  writer = pp.ParquetWriter(fn, table.schema, compression='snappy')
  writer.write_table(table)
  writer.close()


def ffill(arr):
  arr = np.transpose(arr)
  mask = (arr == 0) + np.isnan(arr)
  idx = np.where(~mask, np.arange(mask.shape[1]), 0)
  np.maximum.accumulate(idx, axis=1, out=idx)
  # out = arr[np.arange(idx.shape[0])[:,None], idx]
  arr[mask] = arr[np.nonzero(mask)[0], idx[mask]]
  return np.transpose(arr)


if __name__ == '__main__':
  main()
