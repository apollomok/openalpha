import numpy as np

closePrice = dr.GetData("close")


def Generate(di, alpha):
  alpha[:] = -(closePrice[di - delay, :] - closePrice[di - delay - 2, :])
  alpha[:] = onlyValid(
      alpha,
      di)  # User defined function. Filters out values for invalid instruments


def onlyValid(x, di):
  myValid = valid[di -
                  delay, :]  # myValid vector has yesterday's valid values only.
  return np.where(myValid, x, np.nan)
