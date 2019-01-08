#include <openalpha/alpha.h>

namespace openalpha {

struct Sample : public Alpha {
  void Initialize() override { close_price = dr().GetData("close"); }
  void Generate(int di, double* alpha) override {
    di = di - delay();
    for (auto ii = 0; ii < num_instruments(); ++ii) {
      if (!valid(di, ii)) continue;
      double px_2;
      double px_0;
      close_price.Visit<double>(di, ii, -2, [&](double px, int offset) {
        if (offset == -2)
          px_2 = px;
        else if (!offset)
          px_0 = px;
        return false;
      });
      alpha[ii] = -(px_0 - px_2);
      // alpha[ii] = -(close_price.Value<double>(di, ii) -
      //            close_price.Value<double>(di - 2, ii));
    }
  }
  ArrowTable close_price;
};

}  // namespace openalpha

extern "C" {
openalpha::Alpha* create() { return new openalpha::Sample{}; }
}