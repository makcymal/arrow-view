#include "impl/runtime_config.h"
#include "impl/viewer.h"


int main(int argc, char *argv[]) {
  auto rc = ParseRuntimeConfig(argc, argv);
  ViewArrow(rc);
  return 0;
}
