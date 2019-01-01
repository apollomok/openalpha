#include "alpha.h"

#include <map>

#include "logger.h"

namespace openalpha {

static std::map<std::string, int> kUsedAlphaFileNames;

Alpha* Alpha::Initialize(const std::string& name, ParamMap&& params) {
  name_ = name;
  params_ = std::move(params);
  num_dates_ = dr_.Get("date")->num_rows();
  num_symbols_ = dr_.Get("symbol")->num_rows();
  auto n = num_dates_ * num_symbols_;
  auto raw_alpha = new double[n];  // memory leak
  auto raw_valid = new bool[n];    // memory leak
  auto nan = std::nan("");
  for (auto i = 0lu; i < n; ++i) {
    raw_alpha[i] = nan;
    raw_valid[i] = false;
  }
  alpha_ = new double*[num_dates_];
  valid_ = new bool*[num_dates_];
  for (auto i = 0lu; i < num_dates_; ++i) {
    alpha_[i] = raw_alpha + i * num_symbols_;
    valid_[i] = raw_valid + i * num_symbols_;
  }

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
    auto module = bp::import(module_name.c_str());
    auto dr = bp::object(kOpenAlpha.attr("dr"));
    PyModule_AddStringConstant(module.ptr(), "name", name_.c_str());
    bp::dict py_params;
    for (auto& pair : this->params()) py_params[pair.first] = pair.second;
    PyModule_AddObject(module.ptr(), "dr", dr.ptr());
    auto ptr = py_params.ptr();
    Py_INCREF(ptr);  // memory leak
    PyModule_AddObject(module.ptr(), "params", ptr);
    auto np_valid = np::from_data(
        &valid_[0][0], np::dtype::get_builtin<bool>(),
        bp::make_tuple(num_dates(), num_symbols()),
        bp::make_tuple(num_symbols() * sizeof(bool), sizeof(bool)),
        bp::object());
    ptr = np_valid.ptr();
    Py_INCREF(ptr);  // memory leak
    PyModule_AddObject(module.ptr(), "valid", ptr);
    PyModule_AddIntConstant(module.ptr(), "delay", delay_);
    PyModule_AddIntConstant(module.ptr(), "decay", decay_);
    generate_func_ = GetCallable(module, "generate");
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

  return this;
}

void Alpha::UpdateValid(int di) {
  auto adv60 = dr_.Get("adv60");
  // https://github.com/apache/arrow/blob/master/cpp/examples/arrow/row-wise-conversion-example.cc
  auto array = std::static_pointer_cast<arrow::DoubleArray>(
      adv60->column(di)->data()->chunk(0));
}

void PyAlpha::Generate(int di, double* alpha) {
  auto py_alpha =
      np::from_data(alpha, np::dtype::get_builtin<decltype(alpha[0])>(),
                    bp::make_tuple(num_symbols()),
                    bp::make_tuple(sizeof(alpha[0])), bp::object());
  try {
    generate_func_(di, py_alpha);
  } catch (const bp::error_already_set& err) {
    PrintPyError("Alpha: failed to run '" + GetParam("alpha") + "': ", true,
                 true);
  }
}

void AlphaRegistry::Run() {
  auto num_dates = dr_.Get("date")->num_rows();
  for (auto i = 0l; i < num_dates; ++i) {
    for (auto& pair : alphas_) {
      pair.second->UpdateValid(i);
      pair.second->Generate(i, pair.second->alpha_[i]);
    }
  }
}

}  // namespace openalpha
