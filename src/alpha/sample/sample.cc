#include <openalpha/alpha.h>

namespace openalpha {

struct Sample : public Alpha {
  void Initialize() override { close_price = dr().GetData("close"); }
  void Generate(int di, double* alpha) override {
    di = di - delay();
    for (auto ii = 0; ii < num_instruments(); ++ii) {
      if (!valid(di, ii)) continue;
      alpha[ii] = -(close_price.Value<double>(di, ii) -
                    close_price.Value<double>(di - 2, ii));
    }
  }
  ArrowTable close_price;
};

}  // namespace openalpha

extern "C" {
openalpha::Alpha* create() { return new openalpha::Sample{}; }
}
