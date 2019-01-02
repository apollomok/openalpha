#ifndef OPENALPHA_DATA_H_
#define OPENALPHA_DATA_H_

#include <arrow/python/pyarrow.h>
#include <arrow/table.h>
#include <string>
#include <unordered_map>

#include "common.h"
#include "logger.h"
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

  template <typename T>
  static void Assert(const std::string& name) {
    auto table = DataRegistry::Instance().Get(name);
    if (!table->num_columns()) {
      LOG_FATAL("DataRegistry: empty data of '" << name << "'");
    }
    auto type = table->column(0)->type();
    if (!type) {
      LOG_FATAL("DataRegistry: empty data type of '"
                << name << "', expected "
                << boost::typeindex::type_id<T>().pretty_name());
    }
    bool res;
    if constexpr (std::is_same<T, double>::value) {
      res = type->id() == arrow::Type::DOUBLE;
    } else if constexpr (std::is_same<T, int64_t>::value) {
      res = type->id() == arrow::Type::INT64;
    } else if constexpr (std::is_same<T, uint64_t>::value) {
      res = type->id() == arrow::Type::UINT64;
    } else if constexpr (std::is_same<T, int32_t>::value) {
      res = type->id() == arrow::Type::INT32;
    } else if constexpr (std::is_same<T, uint32_t>::value) {
      res = type->id() == arrow::Type::UINT32;
    } else if constexpr (std::is_same<T, int16_t>::value) {
      res = type->id() == arrow::Type::INT16;
    } else if constexpr (std::is_same<T, uint16_t>::value) {
      res = type->id() == arrow::Type::UINT16;
    } else if constexpr (std::is_same<T, int8_t>::value) {
      res = type->id() == arrow::Type::INT8;
    } else if constexpr (std::is_same<T, uint8_t>::value) {
      res = type->id() == arrow::Type::UINT8;
    } else if constexpr (std::is_same<T, bool>::value) {
      res = type->id() == arrow::Type::BOOL;
    } else if constexpr (std::is_same<T, std::string>::value) {
      res = type->id() == arrow::Type::STRING;
    } else if constexpr (std::is_same<T, float>::value) {
      res = type->id() == arrow::Type::FLOAT;
    } else {
      res = false;
      assert(false);
    }
    if (!res) {
      LOG_FATAL("DataRegistry: invalid data type '"
                << type->name() << "' of '" << name << "', expected '"
                << boost::typeindex::type_id<T>().pretty_name() << "'");
    }
  }

 private:
  ArrayMap array_map_;
  PyArrayMap py_array_map_;
};

}  // namespace openalpha

#endif  // OPENALPHA_DATA_H_
