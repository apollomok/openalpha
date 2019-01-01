#ifndef OPENALPHA_DATA_H_
#define OPENALPHA_DATA_H_

#include <arrow/python/pyarrow.h>
#include <arrow/table.h>
#include <string>
#include <unordered_map>

#include "common.h"
#include "python.h"

namespace openalpha {

class DataRegistry : public Singleton<DataRegistry> {
 public:
  typedef std::shared_ptr<arrow::Table> Array;
  typedef std::unordered_map<std::string, Array> ArrayMap;
  typedef std::unordered_map<std::string, bp::object> PyArrayMap;
  void Initialize();
  Array Get(const std::string& name);
  bp::object GetPy(const std::string& name);

 private:
  ArrayMap array_map_;
  PyArrayMap py_array_map_;
};

}  // namespace openalpha

#endif  // OPENALPHA_DATA_H_
