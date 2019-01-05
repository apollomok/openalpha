#include "python.h"

#include <Python.h>
#include <arrow/python/pyarrow.h>
#include <boost/filesystem.hpp>

#include "data.h"
#include "logger.h"

namespace openalpha {

BOOST_PYTHON_MEMBER_FUNCTION_OVERLOADS(DataRegistry_get_overloads,
                                       DataRegistry::GetPy, 1, 2)

BOOST_PYTHON_MODULE(openalpha) {
  bp::class_<DataRegistry>("DataRegistry", bp::no_init)
      .def("get", &DataRegistry::GetPy,
           DataRegistry_get_overloads(bp::args("name", "retain")));
  bp::scope().attr("dr") = bp::ptr(&DataRegistry::Instance());
}

#if PY_MAJOR_VERSION >= 3
#define INIT_MODULE PyInit_openalpha
extern "C" PyObject *INIT_MODULE();
#else
#define INIT_MODULE initopenalpha
extern "C" void INIT_MODULE();
#endif

void PrintPyError(const char *from, bool fatal, bool traceback) {
  PyObject *ptype, *pvalue, *ptraceback;
  PyErr_Fetch(&ptype, &pvalue, &ptraceback);
  PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
  bp::object type(bp::handle<>(bp::borrowed(ptype)));
  bp::object value(bp::handle<>(bp::borrowed(pvalue)));
  std::string result;
  if (ptraceback && traceback) {
    bp::object tb(bp::handle<>(bp::borrowed(ptraceback)));
    if (ptraceback) {
      bp::object lines =
          bp::import("traceback").attr("format_exception")(type, value, tb);
      for (auto i = 0; i < bp::len(lines); ++i)
        result += bp::extract<std::string>(lines[i])();
    }
  } else {
    std::string errstr = bp::extract<std::string>(bp::str(value));
    std::string typestr =
        bp::extract<std::string>(bp::str(type.attr("__name__")));
    try {
      std::string text = bp::extract<std::string>(value.attr("text"));
      int64_t offset = bp::extract<int64_t>(value.attr("offset"));
      char space[offset + 1] = {};
      for (auto i = 0; i < offset - 1; ++i) space[i] = ' ';
      errstr += "\n" + text + space + "^";
    } catch (const bp::error_already_set &err) {
      PyErr_Clear();
    }
    result = typestr + ": " + errstr;
  }
  PyErr_Restore(ptype, pvalue, ptraceback);
  PyErr_Clear();
  if (fatal) {
    LOG_FATAL(from << "\n" << result);
  } else {
    LOG_ERROR(from << "\n" << result);
  }
}

bp::object GetCallable(const bp::object &m, const char *name) {
  if (!PyObject_HasAttrString(m.ptr(), name)) return {};
  bp::object func = m.attr(name);
  if (!PyCallable_Check(func.ptr())) {
    return {};
  }
  return func;
}

void InitalizePy() {
  PyImport_AppendInittab(const_cast<char *>("openalpha"), INIT_MODULE);
  Py_InitializeEx(0);  // no signal registration
  np::initialize();
  arrow::py::import_pyarrow();
  if (!PyEval_ThreadsInitialized()) PyEval_InitThreads();
  kOpenAlpha = bp::import("openalpha");
  LOG_INFO("Python initialized");
}

}  // namespace openalpha
