#ifndef OPENALPHA_DATA_H_
#define OPENALPHA_DATA_H_

#include <boost/type_index.hpp>
#include <string>
#include <unordered_map>

#include "common.h"
#include "logger.h"
#include "python.h"

namespace openalpha {

class Table {
 public:
  enum Type {
    kUnknown = 0,
    kDouble,
    kFloat,
    kInt64,
    kUInt64,
    kInt32,
    kUInt32,
    kInt16,
    kUInt16,
    kInt8,
    kUInt8,
    kBool,
    kString,
  };

  template <typename T>
  void Assert() {
    bool res;
    if constexpr (std::is_same<T, double>::value) {
      res = type_ == kDouble;
    } else if constexpr (std::is_same<T, float>::value) {
      res = type_ == kFloat;
    } else if constexpr (std::is_same<T, int64_t>::value) {
      res = type_ == kInt64;
    } else if constexpr (std::is_same<T, uint64_t>::value) {
      res = type_ == kUInt64;
    } else if constexpr (std::is_same<T, int32_t>::value) {
      res = type_ == kInt32;
    } else if constexpr (std::is_same<T, uint32_t>::value) {
      res = type_ == kUInt32;
    } else if constexpr (std::is_same<T, int16_t>::value) {
      res = type_ == kInt16;
    } else if constexpr (std::is_same<T, uint16_t>::value) {
      res = type_ == kUInt16;
    } else if constexpr (std::is_same<T, int8_t>::value) {
      res = type_ == kInt8;
    } else if constexpr (std::is_same<T, uint8_t>::value) {
      res = type_ == kUInt8;
    } else if constexpr (std::is_same<T, bool>::value) {
      res = type_ == kBool;
    } else if constexpr (std::is_same<T, std::string>::value) {
      res = type_ == kString;
    } else {
      res = false;
    }
    if (!res) {
      LOG_FATAL("DataRegistry: invalid data type '"
                << boost::typeindex::type_id<T>().pretty_name()
                << "' given for '" << name_ << "' whose type is '" << type_name_
                << "'");
    }
  }

  template <typename T>
  const T* Values(int irow) {
    Assert<T>();
    if (irow >= num_rows_) {
      LOG_FATAL("DataRegistry: row index " << irow << " out of range "
                                           << num_rows_ << " of '" << name_
                                           << "'");
    }
    return reinterpret_cast<T*>(data_) + irow * num_columns_;
  }

  template <typename T>
  T Value(int irow, int icol) {
    auto row = Values<T>(irow);
    if (icol >= num_columns_) {
      LOG_FATAL("DataRegistry: column index "
                << icol << " out of range " << num_columns_ << " of '" << name_
                << "'");
    }
    return row[icol];
  }

  const std::string& name() const { return name_; }
  auto num_rows() const { return num_rows_; }
  auto num_columns() const { return num_columns_; }
  auto type() const { return type_; }
  auto type_name() const { return type_name_; }
  const Table* operator->() const {
    return this;
  }  // for compatibilty with Parquet version
  operator bool() const { return data_; }

 private:
  std::string name_;
  int num_rows_;
  int num_columns_;
  Type type_ = kUnknown;
  std::string type_name_;
  void* data_ = nullptr;
  friend class DataRegistry;
};

class DataRegistry : public Singleton<DataRegistry> {
 public:
  typedef std::unordered_map<std::string, Table> ArrayMap;
  typedef std::unordered_map<std::string, bp::object> PyArrayMap;
  void Initialize();
  bool Has(const std::string& name);
  Table GetData(const std::string& name, bool retain = true);
  bp::object GetDataPy(std::string name, bool retain = true);

 private:
  ArrayMap array_map_;
  PyArrayMap py_array_map_;
};

}  // namespace openalpha

#endif  // OPENALPHA_DATA_H_
