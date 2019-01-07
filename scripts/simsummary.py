#!/usr/bin/env python3
import os
import sys
from collections import defaultdict
from datetime import datetime
from math import sqrt
from glob import glob
import pandas as pd

# last date
ldate = 0
xx_last = 0
ww_last = 0
stats = {}

dd_start = 0
dd_setst = 1
dd_sum = 0


def get_ww(date):
  date = datetime.strptime(date, "%Y%m%d")
  return date.strftime("%W")


def do_stats(xx, date, pnl, long, short, ret, sh_hld, sh_trd, nlong, nshort,
             ntrade, tvr):
  global stats, xx_last, ww_last, ldate

  if stats.get(xx) == None:
    stats[xx] = defaultdict(float)

  if stats[xx].get("dates") == None:
    stats[xx]["dates"] = []

  stats[xx]["dates"].append(date)

  stats[xx]['pnl'] += pnl
  stats[xx]['long'] += long
  stats[xx]['short'] += short
  stats[xx]['nlong'] += nlong
  stats[xx]['nshort'] += nshort
  stats[xx]['ntrade'] += ntrade
  stats[xx]['sh_hld'] += sh_hld
  stats[xx]['sh_trd'] += sh_trd
  if stats[xx]['min_ret'] > ret:
    stats[xx]['min_ret'] = ret
    stats[xx]['min_ret_day'] = date
  if stats[xx]['max_ret'] < ret:
    stats[xx]['max_ret'] = ret
    stats[xx]['max_ret_day'] = date
  stats[xx]['tvr'] += tvr
  stats[xx]['avg_ret'] += ret
  stats[xx]['days'] += 1

  stats[xx]['xsy'] += ret
  stats[xx]['xsyy'] += ret * ret
  if ret > 0:
    stats[xx]['up_days'] += 1

  if stats[xx].get("mm_sum") is None:
    stats[xx]["mm_sum"] = 0
    stats[xx]["mm_cnt"] = 0
    stats[xx]["up_months"] = 0

  if stats[xx].get("ww_sum") is None:
    stats[xx]["ww_sum"] = 0
    stats[xx]["ww_cnt"] = 0
    stats[xx]["up_weeks"] = 0

  if stats[xx].get("drawdown") is None:
    stats[xx]["drawdown"] = 0
    stats[xx]["dd_start"] = 0
    stats[xx]["dd_end"] = 0
  # New month?
  if ldate != 0:
    mm = int(date) / 100 % 100
    mm_last = int(ldate) / 100 % 100
    if mm_last != mm:
      if xx != "ALL":
        if stats[xx_last]["mm_sum"] > 0:
          stats[xx_last]["up_months"] += 1
      else:
        if stats[xx]["mm_sum"] > 0:
          stats[xx]["up_months"] += 1
        stats[xx]["mm_sum"] = 0
  stats[xx]["mm_sum"] += pnl

  # New week?
  if ldate != 0:
    ww = get_ww(date)
    ww_last = get_ww(ldate)

    if ww_last != ww:
      if xx != "ALL":
        if stats[xx_last]["ww_sum"] > 0:
          stats[xx_last]["up_weeks"] += 1
      else:
        if stats[xx]["ww_sum"] > 0:
          stats[xx]["up_weeks"] += 1
        stats[xx_last]["ww_sum"] = 0

  stats[xx]["ww_sum"] += pnl


def get_pnls(fname):
  r = {}
  rpt = pd.read_csv(fname)
  for di in range(len(rpt.date)):
    r[str(rpt.date[di])] = rpt.pnl[di]
  return r


def com_correlation_num(a1, a2):
  from math import sqrt
  n = len(a1)
  if n == 0:
    return 0.0
  s1 = sum(a1)
  s2 = sum(a2)
  s12 = sum([x * y for x, y in zip(a1, a2)])
  r = s12 - (s1 * s2) / n
  den = sqrt((sum(x * x for x in a1) - s1 * s1 / n) *
             (sum(y * y for y in a2) - s2 * s2 / n))
  if den > 0.00001:
    return r / den
  return 0.0


def com_correlation_data(br, cr):
  bdates = set(br.keys())
  cdates = set(cr.keys())
  dates = bdates & cdates
  a1 = [br[date] for date in dates]
  a2 = [cr[date] for date in dates]
  return com_correlation_num(a1, a2)


def com_correlation(fname, others=None):
  fdir = os.path.dirname(fname)
  bname = os.path.basename(fname)

  br = get_pnls(fname)
  r = {}
  if others is None or others == []:
    others = glob(os.path.join(fdir, "*"))
  else:
    others.append(fname)

  for ifilepath in others:
    cr = get_pnls(ifilepath)
    corre = com_correlation_data(br, cr)
    r[os.path.basename(ifilepath)] = corre

  r_ = r.items()
  r_.sort(key=lambda x: x[1])
  for key, value in r_:
    print("%.4f      %s" % (value, key))


def get_plot_data(fname):
  pnls = []
  dates = []
  rpt = pd.read_csv(fname)
  pnl = 0
  for di in range(len(rpt.date)):
    dates.append(str(rpt.date[di]))
    pnl += rpt.pnl[di]
    pnls.append(pnl)
  return dates, pnls


def plot_pnl(fname):
  bname = os.path.basename(fname)
  pnls = []
  dates = []

  import matplotlib.pyplot as plt
  import matplotlib.dates as mdates

  dates, pnls = get_plot_data(fname)

  fig = plt.figure()
  ax = fig.add_subplot(111, title=bname)

  plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
  plt.gca().xaxis.set_major_locator(mdates.DayLocator())
  ax.plot(dates, pnls)
  ax.grid(True)

  fig.autofmt_xdate()

  plt.show()


def plot_all_pnl(fname, others=None):
  fdir = os.path.dirname(fname)
  bname = os.path.basename(fname)

  if fdir == "":
    print("please specify the pnl file with the directory")
    return

  import matplotlib.pyplot as plt
  fig = plt.figure()
  ax = fig.add_subplot(111, title="all")

  names = []
  if others is None or others == []:
    others = glob(os.path.join(fdir, "*"))
  else:
    others.append(fname)

  #
  # create anther global pnl plot
  #
  for ifilepath in others:
    bname = os.path.basename(ifilepath)
    names.append(bname)
    dates, pnls = get_plot_data(ifilepath)

    ax.plot(dates, pnls, label=bname)
    ax.grid(True)

  #plt.legend(names)
  #plt.legend(loc = 2)
  plt.legend(loc=(1.01, 0.1))
  #plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=4,
  #               ncol=2, mode="expand", borderaxespad=0.)

  #plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

  fig.autofmt_xdate()

  plt.show()


def main():
  global ldate, xx_last, dd_start, dd_setst, dd_sum
  ctype = "yearly"
  nargv = len(sys.argv)
  if nargv >= 3:
    fname = sys.argv[1]
    others = sys.argv[2:-1]
    ctype = sys.argv[-1]
    if ctype.lower() == "correlation":
      com_correlation(fname, others)
      return
    elif ctype.lower() == "plot":
      plot_pnl(fname)
      return
    elif ctype.lower() == "plotall":
      plot_all_pnl(fname, others)
      return

  elif nargv == 2:
    fname = sys.argv[-1]
  else:
    print("Usage: %s pnlfile [period]/correlation/plot/plotall" %
          (sys.argv[0],))
    return

  rpt = pd.read_csv(fname)
  for di in range(len(rpt.date)):
    date = str(rpt.date[di])
    long = rpt.long[di]
    short = rpt.short[di]
    ret = rpt.ret[di]
    pnl = rpt.pnl[di]
    sh_hld = rpt.sh_hld[di]
    sh_trd = rpt.sh_trd[di]
    nlong = rpt.nlong[di]
    nshort = rpt.nshort[di]
    tvr = rpt.tvr[di]
    ntrade = rpt.ntrade[di]

    xx = date[:4]
    if ctype.lower() == "monthly":
      xx = date[:6]
    do_stats(xx, date, pnl, long, short, ret, sh_hld, sh_trd, nlong, nshort,
             ntrade, tvr)
    do_stats("ALL", date, pnl, long, short, ret, sh_hld, sh_trd, nlong, nshort,
             ntrade, tvr)

    if dd_setst:
      dd_start = date
      dd_setst = 0
    dd_sum += pnl
    if dd_sum >= 0:
      dd_sum = 0
      dd_start = date
      dd_setst = 1
    if dd_sum < stats[xx]['drawdown']:
      stats[xx]['drawdown'] = dd_sum
      stats[xx]['dd_start'] = dd_start
      stats[xx]['dd_end'] = date
    if dd_sum < stats['ALL']['drawdown']:
      stats['ALL']['drawdown'] = dd_sum
      stats['ALL']['dd_start'] = dd_start
      stats['ALL']['dd_end'] = date

    ldate = date
    xx_last = xx
  print("%8s %8s %10s %10s %10s %8s %10s %8s %8s %8s %6s %6s %6s %8s %5s %5s %5s"% \
        ("date_frm", "date_to", "long", "short", "trade", "tvr", "pnl", "ret/bps", "ret/tvr", "ir", "#long", \
        "#short", "#trade", "perwin", "up_dd", "up_ww", "up_mm"))

  _stats = list(stats.items())
  _stats.sort(key=lambda x: x[0])
  newlines = ""
  for key, vdict in _stats:
    d = vdict.get("days")

    if d <= 0.00001:
      continue
    if key == "ALL":
      if len(_stats) <= 2:
        break
      print("")
      newlines += "\n"

    long = vdict['long'] / d
    short = vdict['short'] / d
    nlong = vdict['nlong'] / d
    nshort = vdict['nshort'] / d
    ntrade = vdict['ntrade'] / d
    ret = vdict['avg_ret'] / d
    perwin = vdict['up_days'] / d
    vsh_hld = vdict['sh_hld']
    turnover = vdict['tvr'] / d
    trade = vdict['sh_trd'] / d
    drawdown = vdict['drawdown']
    pnl = vdict['pnl']

    avg = 0
    std = 0
    ir = 0

    if d > 0:
      avg = stats[key]['xsy'] / d
    if d > 1:
      std = sqrt(1 / (d - 1) *
                 (vdict['xsyy'] - vdict['xsy'] * vdict['xsy'] / d))
    if std > 0:
      ir = avg / std

    print("%8s %8s %10d %10d %10d %8.4f %10d %8.4f %8.4f %8.4f %6d %6d %6d %8.4f %5d %5d %5d" %  \
            (vdict["dates"][0], vdict["dates"][-1], long, short, trade, turnover, pnl, ret,
                ret/turnover if turnover != 0 else 0, ir, nlong, nshort, ntrade,
            perwin, vdict["up_days"], vdict["up_weeks"], vdict["up_months"]))
    newlines += "%10d/%8s-%8s %8.4f/%8s %8.4f/%8s\n" % (drawdown,
                                                        vdict["dd_start"],
                                                        vdict["dd_end"],
                                                        vdict["min_ret"],
                                                        vdict["min_ret_day"],
                                                        vdict["max_ret"],
                                                        vdict["max_ret_day"])
  print("\n%28s %17s %17s" % ("drawdown", "min_ret", "max_ret"))
  print(newlines)


if __name__ == '__main__':
  main()
