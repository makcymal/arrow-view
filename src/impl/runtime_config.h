#pragma once

#include <iostream>
#include <string>


struct RuntimeConfig {
  std::string arrow_file;
  short head_rows = 0;
  bool describe = false, info = false;


  void print() {
    std::cout << "Arrow file: " << arrow_file << std::endl
              << "head_rows: " << head_rows << std::endl
              << "describe: " << describe << std::endl
              << "info: " << info << std::endl;
  }
};


RuntimeConfig ParseRuntimeConfig(int argc, char *argv[]);
