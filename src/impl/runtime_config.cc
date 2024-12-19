#include "runtime_config.h"

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>


namespace po = boost::program_options;


RuntimeConfig ParseRuntimeConfig(int argc, char *argv[]) {
  RuntimeConfig rc;

  po::options_description options(
    "Preview Apache Arrow files\n"
    "Usage: arrow-view [option] arrow-file\n"
    "Options");

  // clang-format off
  options.add_options()
    ("arrow-file", po::value<std::string>(&rc.arrow_file), "Arrow file to read")
    ("head,h", po::value<short>(&rc.head_rows) -> implicit_value(5),
      "First N rows")
    ("describe,d", po::bool_switch(&rc.describe)->default_value(false),
      "Descriptive statistics on numerical columns")
    ("info,i", po::bool_switch(&rc.info)->default_value(false),
      "Overview information: data types and non-null values")
    ("help", "Print help message")
  ;  // clang-format on

  po::positional_options_description positional;
  positional.add("arrow-file", 1);

  try {
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv)
                .options(options)
                .positional(positional)
                .run(),
              vm);
    po::notify(vm);

    if (vm.count("help") or rc.arrow_file.empty()) {
      std::cout << options << std::endl;
      exit(EXIT_SUCCESS);
    }
  } catch (std::exception &ex) {
    std::cout << options << std::endl;
    exit(EXIT_FAILURE);
  }

  return rc;
}
