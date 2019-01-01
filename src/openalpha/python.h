#ifndef OPENALPHA_PYTHON_H_
#define OPENALPHA_PYTHON_H_

#include <boost/python.hpp>
#include <boost/python/numpy.hpp>

namespace bp = boost::python;
namespace np = boost::python::numpy;

namespace openalpha {

void InitalizePy();
void PrintPyError(const char*, bool fatal = false, bool traceback = false);
inline void PrintPyError(const std::string& from, bool fatal = false,
                         bool traceback = false) {
  PrintPyError(from.c_str(), fatal, traceback);
}
bp::object GetCallable(const bp::object& m, const char* name);
inline bp::object kOpenAlpha;

}  // namespace openalpha

#endif  // OPENALPHA_PYTHON_H_
