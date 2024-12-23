#include <arrow/api.h>
#include <iostream>


#include "impl/runtime_config.h"
#include "impl/viewer.h"


int main(int argc, char *argv[]) {
  auto rc = RuntimeConfig(argc, argv);

  arrow::Status st = ViewArrow(rc);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
