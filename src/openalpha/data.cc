#include "data.h"

#include <boost/type_index.hpp>

#include "logger.h"
#include "python.h"

namespace openalpha {

DataRegistry::Array DataRegistry::Get(const std::string& name) {
  auto out = array_map_[name];
  if (out) return out;
  try {
    auto obj = bp::import("pyarrow.parquet")
                   .attr("read_table")((kCachePath / (name + ".par")).string());
    arrow::py::unwrap_table(obj.ptr(), &out);
    array_map_[name] = out;
    LOG_INFO("DataRegistry: " << name << " loaded");
  } catch (const bp::error_already_set& err) {
    PrintPyError("DataRegistry: failed to load '" + name + "': ", true);
  }
  return out;
}

bp::object DataRegistry::GetPy(const std::string& name) {
  auto out = py_array_map_[name];
  if (out) return out;
  auto array = Get(name);
  auto ptr = arrow::py::wrap_table(array);
  out = bp::object(bp::handle<>(bp::borrowed(ptr)))
            .attr("to_pandas")()
            .attr("values");
  assert(Py_REFCNT(ptr) == 1);
  py_array_map_[name] = out;
  return out;
}

template <typename T>
void Assert(const std::string& name) {
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

void DataRegistry::Initialize() {
  GetPy("symbol");
  GetPy("date");
  GetPy("sector");
  GetPy("industry");
  GetPy("subindustry");
  GetPy("adv60");
  Assert<double>("adv60");
}

}  // namespace openalpha
