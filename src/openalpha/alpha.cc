#include "alpha.h"

#include <iomanip>
#include <map>
#include <tuple>

#include "logger.h"

namespace openalpha {

static std::map<std::string, int> kUsedAlphaFileNames;

Alpha* Alpha::Initialize(const std::string& name, ParamMap&& params) {
  name_ = name;
  params_ = std::move(params);
  num_dates_ = dr_.GetData("date")->num_rows();
  num_instruments_ = dr_.GetData("symbol")->num_rows();
  auto n = num_dates_ * num_instruments_;
  auto raw_alpha = new double[n];  // memory leak
  auto raw_valid = new bool[n];    // memory leak
  for (auto i = 0; i < n; ++i) {
    raw_alpha[i] = kNaN;
    raw_valid[i] = false;
  }
  alpha_ = new double*[num_dates_];
  valid_ = new bool*[num_dates_];
  for (auto i = 0; i < num_dates_; ++i) {
    alpha_[i] = raw_alpha + i * num_instruments_;
    valid_[i] = raw_valid + i * num_instruments_;
  }

  auto param = GetParam("delay");
  if (param.size()) delay_ = std::max(0, atoi(param.c_str()));
  param = GetParam("decay");
  if (param.size()) decay_ = std::max(0, atoi(param.c_str()));
  param = GetParam("universe");
  if (param.size()) universe_ = atoi(param.c_str());
  param = GetParam("lookback_days");
  if (param.size()) lookback_days_ = std::max(0, atoi(param.c_str()));
  param = GetParam("book_size");
  if (param.size()) book_size_ = atof(param.c_str());
  param = GetParam("max_stock_weight");
  if (param.size()) max_stock_weight_ = atof(param.c_str());
  param = GetParam("neutralization");
  if (param == kNeutralizationByMarket)
    neutralization_ = kNeutralizationByMarket;
  else if (param == kNeutralizationBySector)
    neutralization_ = kNeutralizationBySector;
  else if (param == kNeutralizationByIndustry)
    neutralization_ = kNeutralizationByIndustry;
  else if (param == kNeutralizationBySubIndustry)
    neutralization_ = kNeutralizationBySubIndustry;
  LOG_INFO("Alpha: " << name << "\ndelay=" << delay_ << "\ndecay=" << decay_
                     << "\nuniverse=" << universe_ << "\nlookback_days="
                     << lookback_days_ << "\nbook_size=" << book_size_
                     << "\nmax_stock_weight=" << max_stock_weight_
                     << "\nneutralization=" << neutralization_);

  auto path = kStorePath / name_;
  if (!fs::exists(path)) fs::create_directory(path);
  os_.open((path / "daily.csv").string().c_str());
  os_ << "date,pnl,ret,tvr,long,short,sh_hld,sh_trd,nlong,nshort,ntrade\n";

  return this;
}

Alpha* PyAlpha::Initialize(const std::string& name, ParamMap&& params) {
  Alpha::Initialize(name, std::move(params));

  auto path = fs::path(GetParam("alpha"));
  bp::import("sys").attr("path").attr("insert")(0, path.parent_path().string());
  auto fn = path.filename().string();
  if (!fs::exists(path)) {
    LOG_FATAL("Alpha: can't open file '" + path.string() + "': No such file");
  }

  auto module_name =
      fn.substr(0, fn.length() - path.extension().string().length());
  auto n = kUsedAlphaFileNames[fn]++;
  if (n > 0) {
    module_name += "__" + std::to_string(n);
    auto fn2 = module_name + ".py";
    auto path2 = "'" + (path.parent_path() / fn2).string() + "'";
    if (system(("[ ! -s " + path2 + " ] && ln -s '" + fn + "' " + path2)
                   .c_str())) {
      // only for avoiding compile warning
    }
    LOG_INFO("Alpha: link '" + path.string() + "' to '" + fn2 + "'");
  }

  try {
    auto dr = bp::object(kOpenAlpha.attr("dr"));
    auto builtins = bp::import("builtins");
    // PyImport_ImportModuleEx with globals/locals does not work, so use
    // builtins as a workaround
    builtins.attr("dr") = dr;
    builtins.attr("name") = name_;
    bp::dict py_params;
    for (auto& pair : this->params()) py_params[pair.first] = pair.second;
    Py_INCREF(py_params.ptr());  // memory leak
    builtins.attr("params") = py_params;
    auto np_valid = np::from_data(
        &valid_[0][0], np::dtype::get_builtin<bool>(),
        bp::make_tuple(num_dates(), num_instruments()),
        bp::make_tuple(num_instruments() * sizeof(bool), sizeof(bool)),
        bp::object());
    Py_INCREF(np_valid.ptr());  // memory leak
    builtins.attr("valid") = np_valid;
    builtins.attr("delay") = delay_;
    builtins.attr("decay") = decay_;
    bp::object module = bp::import(module_name.c_str());
    PyModule_AddStringConstant(module.ptr(), "name", name_.c_str());
    PyModule_AddObject(module.ptr(), "dr", dr.ptr());
    PyModule_AddObject(module.ptr(), "params", py_params.ptr());
    PyModule_AddObject(module.ptr(), "valid", np_valid.ptr());
    PyModule_AddIntConstant(module.ptr(), "delay", delay_);
    PyModule_AddIntConstant(module.ptr(), "decay", decay_);
    generate_func_ = GetCallable(module, "Generate");
    if (!generate_func_) {
      LOG_FATAL("Alpha: 'generate' function not defined in '" + path.string() +
                "'");
    }
    LOG_INFO("Alpha: '" << path.string() << "' loaded");
  } catch (const bp::error_already_set& err) {
    PrintPyError("Alpha: failed to load '" + path.string() + "': ", true, true);
  }
  bp::import("sys").attr("path").attr("reverse");
  bp::import("sys").attr("path").attr("pop");
  bp::import("sys").attr("path").attr("reverse");

  int_array_.resize(num_instruments_);
  double_array_.resize(num_instruments_);
  pos_.resize(num_instruments_, kNaN);
  stats_.resize(num_dates_);
  date_ = dr_.GetData("date").Values<int64_t>(0);

  return this;
}

void Alpha::UpdateValid(int di) {
  auto values = dr_.GetData("adv60_t").Values<double>(di - delay_);
  for (auto i = 0u; i < int_array_.size(); ++i) int_array_[i] = i;
  std::sort(int_array_.begin(), int_array_.end(),
            [&values](auto i, auto j) { return values[i] < values[j]; });
  auto valid = valid_[di - delay_];
  auto n = 0;
  for (auto it = int_array_.rbegin(); it != int_array_.rend() && n < universe_;
       ++it, ++n) {
    valid[*it] = true;
  }
}

void Alpha::Calculate(int di) {
  auto groups =
      neutralization_ != kNeutralizationByMarket
          ? dr_.GetData(neutralization_ + "_t").Values<int64_t>(di - delay_)
          : nullptr;
  auto alpha = alpha_[di];
  auto valid = valid_[di - delay_];
  std::map<int64_t, std::vector<int>> grouped;
  auto pos_1 = double_array_;
  auto close = dr_.GetData("close_t");
  auto close0 = close.Values<double>(di);
  auto close1 = close.Values<double>(di + 1);
  for (auto ii = 0; ii < num_instruments_; ++ii) {
    pos_1[ii] = pos_[ii];
    pos_[ii] = kNaN;
    auto v = alpha[ii];
    if (!valid[ii]) continue;
    if (std::isnan(alpha[ii])) continue;
    auto ig = groups ? groups[ii] : 0;
    if (ig < 0) continue;
    if (decay_ > 1) {
      auto nsum = decay_;
      auto sum = decay_ * v;
      for (auto j = 1; j < decay_; ++j) {
        auto di2 = di - j;
        if (di2 < 0) continue;
        auto v2 = alpha_[di2][ii];
        if (std::isnan(v2)) continue;
        auto n = decay_ - j;
        nsum += n;
        sum += v2 * n;
      }
      v = sum / nsum;
    }
    pos_[ii] = v;
    grouped[ig].push_back(ii);
  }

  double sum;
  auto max_try = 10;
  for (auto itry = 0; itry <= max_try; ++itry) {
    sum = 0;
    for (auto& pair : grouped) {
      if (pair.second.size() == 1) {
        auto ii = pair.second[0];
        pos_[ii] = kNaN;
        continue;
      }
      auto sum2 = 0.;
      for (auto ii : pair.second) sum2 += pos_[ii];
      auto avg = sum2 / pair.second.size();
      for (auto ii : pair.second) {
        pos_[ii] -= avg;
        sum += std::abs(pos_[ii]);
      }
    }
    if (sum == 0) return;
    if (max_stock_weight_ <= 0 || itry == max_try) break;
    auto max_value = max_stock_weight_ * sum;
    auto threshold = max_value * 1.01;
    auto breach = false;
    for (auto v : pos_) {
      if (std::isnan(v)) continue;
      if (std::abs(v) > threshold) {
        breach = true;
        break;
      }
    }
    if (!breach) break;
    for (auto& v : pos_) {
      if (std::isnan(v)) continue;
      if (std::abs(v) > max_value) v = max_value * (v > 0 ? 1 : -1);
    }
  }

  auto pnl = 0.;
  auto long_pos = 0.;
  auto short_pos = 0.;
  auto sh_hld = 0.;
  auto nlong = 0.;
  auto nshort = 0.;
  for (auto ii = 0; ii < num_instruments_; ++ii) {
    auto& v = pos_[ii];
    if (std::isnan(v)) continue;
    v = std::round(v / sum * book_size_);
    auto px0 = close0[ii];
    auto px1 = close1[ii];
    auto ret = !(px0 > 0) || !(px1 > 0) ? 0 : (px1 / px0 - 1);
    pnl += v * ret;
    if (v > 0) {
      long_pos += v;
      nlong++;
    } else if (v < 0) {
      short_pos -= v;
      nshort++;
    }
    if (px0 > 0) sh_hld += std::abs(v) / px0;
  }
  // In WebSim, return = annualized PnL / half of book size.
  auto ret = pnl / (book_size_ / 2);

  auto tvr = 0.;
  auto ntrade = 0;
  auto sh_trd = 0.;
  for (auto ii = 0; ii < num_instruments_; ++ii) {
    auto v = pos_[ii];
    if (std::isnan(v)) v = 0;
    auto v_1 = pos_1[ii];
    if (std::isnan(v_1)) v_1 = 0;
    auto x = std::abs(v - v_1);
    tvr += x;
    if (x != 0) ntrade++;
    auto px = close0[ii];
    if (px > 0) sh_trd += x / px;
  }
  tvr /= book_size_ * 2;
  auto& st = stats_[di];
  st.ret = ret;
  st.tvr = tvr;
  st.date = date(di);
  st.long_pos = long_pos;
  st.short_pos = short_pos;
  st.nlong = nlong;
  st.nshort = nshort;
  st.pnl = pnl;
  os_ << std::setprecision(15) << st.date << ',' << pnl << ',' << ret << ','
      << tvr << ',' << long_pos << ',' << short_pos << ',' << round(sh_hld)
      << ',' << round(sh_trd) << ',' << nlong << ',' << nshort << ',' << ntrade
      << '\n';
}

static void Report(const std::string& year,
                   const std::vector<Alpha::Stats>& sts, std::ostream& os) {
  if (sts.empty()) return;
  auto dd_reset = true;
  auto dd_sum = 0.;
  auto dd_start = 0;
  auto dd_sum_max = 0.;
  auto dd_start_max = 0.;
  auto dd_end_max = 0;
  auto tvr = 0.;
  auto ret_sum = 0.;
  auto ret_sum2 = 0.;
  auto long_pos = 0.;
  auto short_pos = 0.;
  auto nlong = 0l;
  auto nshort = 0l;
  auto pnl = 0.;
  for (auto& t : sts) {
    auto ret = t.ret;
    ret_sum2 += ret * ret;
    ret_sum += ret;
    auto date = t.date;
    if (dd_reset) {
      dd_start = date;
      dd_reset = false;
    }
    dd_sum += t.pnl;
    if (dd_sum >= 0) {
      dd_sum = 0;
      dd_start = date;
      dd_reset = true;
    }
    if (dd_sum < dd_sum_max) {
      dd_sum_max = dd_sum;
      dd_start_max = dd_start;
      dd_end_max = date;
    }
    tvr += t.tvr;
    long_pos += t.long_pos;
    short_pos += t.short_pos;
    nlong += t.nlong;
    nshort += t.nshort;
    pnl += t.pnl;
  }

  auto stddev = 0.;
  auto ir = 0.;
  auto d = sts.size();
  auto avg = ret_sum / d;
  if (d > 1) stddev = sqrt(1. / (d - 1) * (ret_sum2 - ret_sum * ret_sum / d));
  if (stddev > 0) ir = avg / stddev;
  tvr /= d;
  auto sharp = ir * sqrt(252);
  auto ret = ret_sum / d * 252;
  auto fitness = sharp * sqrt(std::abs(ret) / tvr);
  os << year << ',' << pnl << ',' << (ret_sum / d) << ',' << ir << ','
     << dd_sum_max << ',' << dd_start_max << ',' << dd_end_max << ',' << tvr
     << ',' << (long_pos / d) << ',' << (short_pos / d) << ',' << (nlong / d)
     << ',' << (nshort / d) << ',' << fitness << '\n';
}

void Alpha::Report() {
  std::vector<Stats> sts;
  std::map<int, std::vector<Stats>> yearly;
  for (auto di = 0; di < num_dates_; ++di) {
    auto& st = stats_[di];
    if (std::isnan(st.ret)) continue;
    auto d = date(di);
    sts.push_back(st);
    yearly[d / 10000].push_back(st);
  }
  os_.close();
  auto path = kStorePath / name();
  LOG_INFO("Alpha: dump daily results: " << (path / "daily.csv"));
  path = path / "perf.csv";
  os_.open(path.string().c_str());
  os_ << "date,pnl,ret,ir,dd,dd_start,dd_end,tvr,long,short,"
         "nlong,nshort,fitness\n";
  for (auto& pair : yearly)
    openalpha::Report(std::to_string(pair.first), pair.second, os_);
  openalpha::Report(std::to_string(yearly.begin()->first) + "-" +
                        std::to_string(yearly.rbegin()->first),
                    sts, os_);
  os_.close();
  try {
    LOG_INFO(
        "Alpha: dump performance report: "
        << path << "\n"
        << sts.begin()->date << "-" << sts.rbegin()->date
        << " book_size=" << book_size_ << " tvr=trade_volume/2/book_size\n"
        << bp::extract<const char*>(bp::str(bp::import("pyarrow.csv")
                                                .attr("read_csv")(path.string())
                                                .attr("to_pandas")())));
  } catch (const bp::error_already_set& err) {
    PrintPyError("");
  }
}

void PyAlpha::Generate(int di, double* alpha) {
  auto py_alpha =
      np::from_data(alpha, np::dtype::get_builtin<decltype(alpha[0])>(),
                    bp::make_tuple(num_instruments()),
                    bp::make_tuple(sizeof(alpha[0])), bp::object());
  try {
    generate_func_(di, py_alpha);
  } catch (const bp::error_already_set& err) {
    PrintPyError("Alpha: failed to run '" + GetParam("alpha") + "': ", true,
                 true);
  }
}

void AlphaRegistry::Run() {
  auto num_dates = dr_.GetData("date")->num_rows();
  for (auto di = 0; di < num_dates - 1; ++di) {
    for (auto& pair : alphas_) {
      auto alpha = pair.second;
      if (di < alpha->lookback_days_ + alpha->delay_) continue;
      alpha->UpdateValid(di);
      alpha->Generate(di, alpha->alpha_[di]);
      alpha->Calculate(di);
    }
  }
  for (auto& pair : alphas_) pair.second->Report();
}

}  // namespace openalpha
