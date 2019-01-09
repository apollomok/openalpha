#include "data.h"

#include <H5Cpp.h>

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
    auto type_class = dataset.getTypeClass();
    auto dataspace = dataset.getSpace();
    auto rank = dataspace.getSimpleExtentNdims();
    assert(rank == 2);
    hsize_t dims_out[2];
    auto ndims = dataspace.getSimpleExtentDims(dims_out, nullptr);
    if (type_class == H5T_INTEGER) {
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
  auto path = kDataPath / (name + ".par");
  return fs::exists(path);
}

bp::object DataRegistry::GetDataPy(std::string name, bool retain) {
  auto out = py_array_map_[name];
  if (retain && out != bp::object{}) return out;
  auto array = GetData(name, retain);
  if (retain) py_array_map_[name] = out;
  return out;
}

void DataRegistry::Initialize() {
  GetDataPy("symbol");
  GetDataPy("date");
}

}  // namespace openalpha
