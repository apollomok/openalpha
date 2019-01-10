#include "data.h"

#include <H5Cpp.h>
#include <iostream>

#include "python.h"

namespace openalpha {

static H5std_string kDatasetName("default");

Table DataRegistry::GetData(const std::string& name, bool retain) {
  auto out = array_map_[name];
  if (retain && out) return out;
  try {
    H5::H5File file(H5std_string((kDataPath / (name + ".h5")).string()),
                    H5F_ACC_RDONLY);
    auto dataset = file.openDataSet(kDatasetName);
    auto dataspace = dataset.getSpace();
    hsize_t dims_out[2];
    auto ndims = dataspace.getSimpleExtentDims(dims_out, nullptr);
    if (ndims != 2) {
      LOG_FATAL("DataRegistry: '" << name << "' is not 2d array");
    }
    auto n = dims_out[0] * dims_out[1];
    out.num_rows_ = dims_out[0];
    out.num_columns_ = dims_out[1];
    out.name_ = name;
    auto data_type = dataset.getDataType();
    auto type_class = data_type.getClass();
    out.type_name_ = data_type.fromClass();
    auto data_size = data_type.getSize();
    void* raw = new char[n * data_size]();
    dataset.read(raw, data_type);
    if (type_class == H5T_FLOAT) {
      if (data_size == 4) {
        out.type_ = Table::kFloat;
      } else if (data_size == 8) {
        out.type_ = Table::kDouble;
      } else {
        LOG_FATAL("DataRegistry: failed to load '" + name +
                      "': unsupported double type with data size = "
                  << data_size);
      }
      out.data_ = std::make_shared<Table::RawData>();
      out.data_->ptr = raw;
    } else if (type_class == H5T_INTEGER) {
      if (data_size == 1) {
        out.type_ = Table::kInt8;
      } else if (data_size == 2) {
        out.type_ = Table::kInt16;
      } else if (data_size == 4) {
        out.type_ = Table::kInt32;
      } else if (data_size == 8) {
        out.type_ = Table::kInt64;
      } else {
        LOG_FATAL("DataRegistry: failed to load '" + name +
                      "': unsupported int type with data size = "
                  << data_size);
      }
      out.data_ = std::make_shared<Table::RawData>();
      out.data_->ptr = raw;
    } else if (type_class == H5T_STRING) {
      out.type_ = Table::kString;
      std::string* strs = new std::string[n];
      for (auto i = 0u; i < n; ++i) {
        auto len = data_size;
        auto c_str = reinterpret_cast<char*>(raw) + data_size * i;
        for (auto p = c_str + len - 1; p != c_str && !*p; --p) --len;
        strs[i].assign(c_str, len);
      }
      out.data_ = std::make_shared<Table::DataTmpl<std::string>>();
      out.data_->ptr = strs;
      free(raw);
    } else {
      LOG_FATAL("DataRegistry: '" << name << "' has data type of '"
                                  << data_type.fromClass()
                                  << "' which is not supported in openalpha");
    }
    if (retain) array_map_[name] = out;
    LOG_INFO("DataRegistry: " << name << " loaded");
  } catch (H5::Exception& err) {
    LOG_FATAL(
        "DataRegistry: failed to load '" + name + "': " << err.getCDetailMsg());
  }
  return out;
}

bool DataRegistry::Has(const std::string& name) {
  auto path = kDataPath / (name + ".h5");
  return fs::exists(path);
}

template <typename T>
inline auto FromData(const Table& tbl) {
  return np::from_data(tbl.Data<void>(), np::dtype::get_builtin<T>(),
                       bp::make_tuple(tbl.num_rows(), tbl.num_columns()),
                       bp::make_tuple(tbl.num_columns() * sizeof(T), sizeof(T)),
                       bp::object());
}

bp::object DataRegistry::GetDataPy(std::string name, bool retain) {
  auto out = py_array_map_[name];
  if (retain && out != bp::object{}) return out;
  auto tbl = GetData(name, retain);
  switch (tbl.type_) {
    case Table::kDouble:
      out = FromData<double>(tbl);
      break;
    case Table::kFloat:
      out = FromData<float>(tbl);
      break;
    case Table::kInt64:
      out = FromData<int64_t>(tbl);
      break;
    case Table::kInt32:
      out = FromData<int32_t>(tbl);
      break;
    case Table::kInt16:
      out = FromData<int16_t>(tbl);
      break;
    case Table::kInt8:
      out = FromData<int8_t>(tbl);
      break;
    case Table::kString:
      LOG_FATAL("DataRegistry: failed to load '" + name +
                "': not support string array in python API yet");
      break;
    default:
      assert(0);
      break;
  }
  if (retain) py_array_map_[name] = out;
  return out;
}

void DataRegistry::Initialize() {
  GetData("symbol");
  GetData("date");
}

}  // namespace openalpha
