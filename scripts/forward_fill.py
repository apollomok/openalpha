#!/usr/bin/env python3

from transpose import *


def ffill(arr):
  arr = np.transpose(arr)
  mask = (arr == 0) + np.isnan(arr)
  idx = np.where(~mask, np.arange(mask.shape[1]), 0)
  np.maximum.accumulate(idx, axis=1, out=idx)
  # out = arr[np.arange(idx.shape[0])[:,None], idx]
  arr[mask] = arr[np.nonzero(mask)[0], idx[mask]]
  return np.transpose(arr)


def main():
  fn = sys.argv[1]
  x = pd.read_parquet(fn).values
  write_array(fn, ffill(x))


if __name__ == '__main__':
  main()
