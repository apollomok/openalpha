#include <openalpha/alpha.h>

namespace openalpha {

struct Sample : public Alpha {
  void Initialize() override { close_price = dr().GetData("close"); }
  void Generate(int di, double* alpha) override {
    di = di - delay();
    auto close_2 = close_price.Row<double>(di - 2);
    auto close_0 = close_price.Row<double>(di);
    for (auto ii = 0; ii < num_instruments(); ++ii) {
      if (!valid(di, ii)) continue;
      double px_2 = close_2[ii];
      double px_0 = close_0[ii];
      if (px_2 > 0 && px_0 > 0) alpha[ii] = -(px_0 - px_2);
    }
  }
  Table close_price;
};

}  // namespace openalpha

extern "C" {
openalpha::Alpha* create() { return new openalpha::Sample{}; }
}
