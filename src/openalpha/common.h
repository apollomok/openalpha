#ifndef OPENALPHA_COMMON_H_
#define OPENALPHA_COMMON_H_

#include <boost/filesystem.hpp>
#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace openalpha {

namespace fs = boost::filesystem;
static inline fs::path kDataPath = fs::path(".") / "data";
static inline fs::path kStorePath = fs::path(".") / "store";
static const double kNaN = std::nan("");

template <typename V>
class Singleton {
 public:
  static V& Instance() {
    static V kInstance;
    return kInstance;
  }

 protected:
  Singleton() {}
};

template <typename V>
const typename V::mapped_type& FindInMap(const V& map,
                                         const typename V::key_type& key) {
  static const typename V::mapped_type kValue{};
  auto it = map.find(key);
  if (it == map.end()) return kValue;
  return it->second;
}

template <typename V>
const typename V::mapped_type& FindInMap(std::shared_ptr<V> map,
                                         const typename V::key_type& key) {
  static const typename V::mapped_type kValue{};
  if (!map) return kValue;
  auto it = map->find(key);
  if (it == map->end()) return kValue;
  return it->second;
}

template <typename T>
std::vector<int> ArgSort(const T& in, size_t n) {
  std::vector<int> out(n, 0);
  for (auto i = 0u; i < n; ++i) out[i] = i;
  std::sort(out.begin(), out.end(),
            [&in](auto i, auto j) { return in[i] < in[j]; });
  return out;
}

}  // namespace openalpha

#endif  // OPENALPHA_COMMON_H_
