#include <dlfcn.h>
#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <fstream>
#include <iostream>

#include "alpha.h"
#include "common.h"
#include "data.h"
#include "logger.h"
#include "python.h"

namespace bpo = boost::program_options;

int main(int argc, char *argv[]) {
  std::string config_file_path;
  std::string log_config_file_path;
  std::string cache_path;
  try {
    bpo::options_description config("Configuration");
    config.add_options()("help,h", "produce help message")(
        "config_file,c",
        bpo::value<std::string>(&config_file_path)
            ->default_value("openalpha.conf"),
        "config file path")("log_config_file,l",
                            bpo::value<std::string>(&log_config_file_path)
                                ->default_value("log.conf"),
                            "log4cxx config file path")(
        "cache_path,C",
        bpo::value<std::string>(&cache_path)
            ->default_value(openalpha::kCachePath.string()),
        "directory path where cache files are located");

    bpo::options_description config_file_options;
    config_file_options.add(config);
    bpo::variables_map vm;

    bpo::store(bpo::parse_command_line(argc, argv, config), vm);
    bpo::notify(vm);
    if (vm.count("help")) {
      std::cerr << config << std::endl;
      return 1;
    }

    std::ifstream ifs(config_file_path.c_str());
    if (ifs) {
      bpo::store(parse_config_file(ifs, config_file_options, true), vm);
    } else {
      std::cerr << config_file_path << " not found" << std::endl;
      return 1;
    }

    bpo::store(bpo::parse_command_line(argc, argv, config), vm);
    bpo::notify(vm);  // make command line option higher priority
  } catch (bpo::error &e) {
    std::cerr << "Bad Options: " << e.what() << std::endl;
    return 1;
  }

  if (!std::ifstream(log_config_file_path.c_str()).good()) {
    std::ofstream(log_config_file_path)
        .write(openalpha::kDefaultLogConf, strlen(openalpha::kDefaultLogConf));
  }

  if (!openalpha::fs::exists(openalpha::kStorePath)) {
    openalpha::fs::create_directory(openalpha::kStorePath);
  }
  openalpha::kCachePath = cache_path;
  openalpha::Logger::Initialize("openalpha", log_config_file_path);
  openalpha::InitalizePy();
  openalpha::DataRegistry::Instance().Initialize();

  auto &ar = openalpha::AlphaRegistry::Instance();
  boost::property_tree::ptree prop_tree;
  boost::property_tree::ini_parser::read_ini(config_file_path, prop_tree);
  for (auto &section : prop_tree) {
    if (!section.second.size()) continue;
    openalpha::Alpha::ParamMap params;
    for (auto &item : section.second) {
      auto name = item.first;
      boost::to_lower(name);
      params[name] = item.second.data();
    }
    auto path = params["alpha"];
    if (!path.empty()) {
      if (boost::algorithm::ends_with(path, ".py")) {
        ar.Add((new openalpha::PyAlpha)
                   ->Initialize(section.first, std::move(params)));
      } else if (boost::algorithm::ends_with(path, ".so")) {
        auto handle = dlopen(path.c_str(), RTLD_NOW);
        if (!handle) {
          LOG_FATAL("Alpha: failed to load '" << path + "': " << dlerror());
        }
        typedef openalpha::Alpha *(*CFunc)();
        auto create_func = (CFunc)dlsym(handle, "create");
        if (!create_func) {
          LOG_FATAL("Alpha: failed to load '" << path + "': " << dlerror());
        }
        auto out = create_func();
        auto alpha = dynamic_cast<openalpha::Alpha *>(out);
        if (!alpha) {
          LOG_FATAL("Alpha: failed to load '"
                    << path + "', create() does not return Alpha object");
        }
        ar.Add(alpha->Initialize(section.first, std::move(params)));
      } else {
        LOG_FATAL("Alpha: invalid path file '"
                  << path << "', expected '.py' or '.so' file")
      }
    }
  }
  ar.Run();

  return 0;
}
