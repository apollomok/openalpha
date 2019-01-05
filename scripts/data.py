#!/usr/bin/env python3

import pyarrow.parquet as pp
import sys
import numpy as np
import pyarrow as pa
import pandas as pd
from optparse import OptionParser
import os


def main():
  actions = ['ffill', 'validate', 'transpose', 'symbol', 'date']
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
  if action == 'symbol':
    dir = args[0]
    arr = pd.read_parquet(os.path.join(dir, 'symbol.par')).values
    symbol = args[1]
    try:
      symbol = int(symbol)
    except:
      pass
    if isinstance(symbol, str):
      print(np.argwhere(arr == symbol)[0, 0])
    elif isinstance(symbol, int):
      print(arr[symbol, 0])
    return
  elif action == 'date':
    dir = args[0]
    arr = pd.read_parquet(os.path.join(dir, 'date.par')).values
    date = int(args[1])
    if date > 10000:
      print(np.argwhere(arr == date)[0, 0])
    else:
      print(arr[date, 0])
    return
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
  arr0[np.isnan(arr0)] = 0
  arr = ffill(arr)
  arr[np.isnan(arr)] = 0
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
    for di in range(1, len(col)):
      px0 = col[di - 1]
      px = col[di]
      if not (px0 > 0): continue
      x = px / px0
      if x > 10 or x < 0.1:
        diff.append((di, ii, int(100 * x)))
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
  '''
[[ 0.,  0.,  0.],         [[0., 0., 0.],
 [ 1.,  2.,  3.],    =>    [1., 2., 3.],
 [ 0.,  2.,  0.],          [1., 2., 3.],
 [ 0.,  4., nan]]          [1., 4., 3.]])
'''

  mask = (arr <= 0) + np.isnan(arr)
  idx = np.where(~mask, np.arange(mask.shape[0])[:, None], 0)
  np.maximum.accumulate(idx, axis=0, out=idx)
  arr[mask] = arr[idx[mask], np.nonzero(mask)[1]]
  return arr


def backward_adj(split):
  '''
     a              =>          b
[[ 0.,  0.,  0.],         [[ 5., 16.,  3.],
 [ 5.,  2.,  3.],   =>     [ 1.,  8.,  1.],
 [ 0.,  2.,  0.],          [ 1.,  4.,  1.],
 [ 0.,  4., nan]])         [ 1.,  1.,  1.]])
 adjusted_px = unadjusted_px / b
 adjusted_vol = unadjusted_vol * b
'''

  split[(split <= 0) + np.isnan(split)] = 1.
  split = np.roll(split, -1, 0)
  split = np.flip(split, 0)
  split[0, :] = 1
  np.multiply.accumulate(split, axis=0, out=split)
  return np.flip(split, 0)


def fill_subindustry(dir):
  fn = os.path.join(dir, 'subindustry.par')
  sub = pd.read_parquet(fn).values
  mask = (sub <= 0) + np.isnan(sub)
  if not len(np.argwhere(mask)): return
  ind = pd.read_parquet(os.path.join(dir, 'industry.par')).values
  sub[mask] = ind[mask]
  write_array(fn, sub)


if __name__ == '__main__':
  main()
