#!/usr/bin/env python3
'''
This is simplified pure-python version of openalpha for testing
'''

from optparse import OptionParser
import os
import sys
import builtins
import pandas as pd
import numpy as np
from collections import defaultdict


def main():
  neutralizations = ['market', 'sector', 'industry', 'subindustry']
  parser = OptionParser(
      usage='usage: %prog [options] filename', version='%prog 1.0')
  parser.add_option(
      '-u', '--universe', type='int', default=3000, help='universe')
  parser.add_option(
      '-l', '--lookback_days', type='int', default=256, help='lookback days')
  parser.add_option('', '--delay', type='int', default=1, help='delay')
  parser.add_option('', '--decay', type='int', default=4, help='decay')
  parser.add_option(
      '',
      '--max_stock_weight',
      type='float',
      default=0.1,
      help='maxium stock weight')
  parser.add_option(
      '', '--book_size', type='float', default=2e7, help='book size')
  parser.add_option(
      '-n',
      '--neutralization',
      type='choice',
      default='subindustry',
      choices=neutralizations,
      help='neutralization method, choose from [' + ', '.join(neutralizations) +
      ']')
  parser.add_option(
      '',
      '--dir_data',
      default='./data',
      help='directory path where data files are located')
  (options, args) = parser.parse_args()
  if not args: return
  sys.path.insert(0, os.path.dirname(args[0]))
  dr = DataRegistry(options.dir_data)
  builtins.dr = dr
  delay = options.delay
  builtins.delay = delay
  module = __import__(os.path.basename(args[0])[:-3])
  instruments = dr.GetData('symbol')[:, 0]
  dates = dr.GetData('date')[:, 0]
  valids = np.zeros(len(instruments) * len(dates)).reshape(
      len(dates), len(instruments))
  valids[:, :] = False
  alphas = np.copy(valids)
  alphas[:, :] = np.nan
  adv60_t = dr.GetData('adv60_t')
  builtins.valid = valids
  groups = None
  if options.neutralization != 'market':
    groups = dr.GetData(options.neutralization + '_t')
  pos = np.zeros(len(instruments))
  pos[:] = np.nan
  old_pos = np.copy(pos)
  close = dr.GetData('close_t')
  days = 0
  pnl = 0.
  tvr = 0.
  nlong = 0
  nshort = 0
  long = 0.
  short = 0.
  print(
      'delay=%d, decay=%d, universe=%d, lookback_days=%d, book_size=%fm, max_stock_weight=%f, neutralization=%s'
      % (delay, options.decay, options.universe, options.lookback_days,
         options.book_size / 1e6, options.max_stock_weight,
         options.neutralization))
  out = open('daily.csv', 'wt')
  out.write('date,pnl,tvr,nlong,nshort,long,short\n')
  for di in range(options.lookback_days + options.delay, len(dates) - 1):
    valid = valids[di - delay, :]
    update_valid(valid, options.universe, adv60_t[:, di - delay])
    module.Generate(di, alphas[di, :])
    res = calculate(di, alphas, old_pos, pos, groups[:, di - delay]
                    if groups is not None else None, options.decay, valid,
                    options.book_size, options.max_stock_weight, close)
    if not res: continue
    days += 1
    out.write('%d,%s\n' % (dates[di], ','.join([str(x) for x in res])))
    pnl += res[0]
    tvr += res[1]
    nlong += res[2]
    nshort += res[3]
    long += res[4]
    short += res[5]
  out.close()
  tvr /= options.book_size * 2 * days
  print('pnl=%fm tvr=%f nlong=%d nshort=%d long=%fm short=%fm' %
        (pnl / 1e6, tvr, int(nlong / days), int(nshort / days),
         long / days / 1e6, short / days / 1e6))


def calculate(di, alphas, old_pos, pos, groups, decay, valid, book_size,
              max_stock_weight, close):
  old_pos[:] = pos[:]
  pos[:] = np.nan
  grouped = defaultdict(list)
  for ii in range(len(pos)):
    if not valid[ii]: continue
    ig = groups[ii] if groups is not None else 0
    if ig < 0: continue
    alpha = np.copy(alphas[:, ii][max(0, di + 1 - decay):di + 1])
    if np.isnan(alpha[-1]): continue
    mask = np.isnan(alpha)
    alpha[mask] = 0
    weight = np.array(range(decay + 1 - len(alpha), decay + 1))
    weight[mask] = 0
    pos[ii] = np.sum(alpha * weight) / np.sum(weight)
    grouped[ig].append(ii)
  max_try = 10
  sum = 0.
  for itry in range(max_try + 1):
    sum = 0
    for group in grouped.values():
      if len(group) == 1:
        pos[group[0]] = np.nan
        continue
      avg = np.sum(pos[group]) / len(group)
      for ii in group:
        pos[ii] -= avg
        sum += abs(pos[ii])
    if sum == 0: return
    if max_stock_weight <= 0 or itry == max_try: break
    max_value = max_stock_weight * sum
    threshold = max_value * 1.01
    breach = False
    for v in pos:
      if np.isnan(v): continue
      if abs(v) > threshold:
        breach = True
        break
    if not breach: break
    for ii in range(len(pos)):
      v = pos[ii]
      if np.isnan(v): continue
      if abs(v) > max_value: pos[ii] = max_value * (1 if v > 0 else -1)
  pnl = 0
  nlong = 0
  nshort = 0
  long = 0
  short = 0
  for ii in range(len(pos)):
    if np.isnan(pos[ii]): continue
    p = round(pos[ii] / sum * book_size)
    pos[ii] = p
    px1 = close[ii, di + 1]
    px0 = close[ii, di]
    ret = (px1 / px0 - 1) if (px1 > 0 and px0 > 0) else 0
    pnl += p * ret
    if p > 0:
      nlong += 1
      long += p
    elif p < 0:
      nshort += 1
      short += p

  tvr = 0
  for ii in range(len(pos)):
    v = pos[ii]
    if np.isnan(v): v = 0
    v_1 = old_pos[ii]
    if np.isnan(v_1): v_1 = 0
    tvr += abs(v - v_1)

  return pnl, tvr, nlong, nshort, long, short


def update_valid(valid, universe, adv60):
  valid[np.argsort(adv60)[-universe:]] = True


class DataRegistry:

  def __init__(self, path):
    self.__path = path

  def GetData(self, name):
    return pd.read_parquet(os.path.join(self.__path, name + '.par')).values


if __name__ == '__main__':
  main()
