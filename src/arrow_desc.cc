#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <iostream>

#include "utils.h"


arrow::Status ArrowDesc(const std::string &arrow_file) {
  return arrow::Status::OK();
}


int main(int argc, char *argv[]) {
  std::string arrow_file = ParseArgs(argc, argv);
  arrow::Status st = ArrowDesc(arrow_file);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
