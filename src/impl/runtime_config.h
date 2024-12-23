#pragma once

#include <iostream>
#include <string>


struct RuntimeConfig {
  std::string arrow_file;
  short head_rows = 0;
  bool describe = false, info = false;

  RuntimeConfig(int argc, char *argv[]);

  void print();
};
