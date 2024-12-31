#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/type.h>

#include <concepts>
#include <random>
#include <string>
#include <vector>

#include "src/lib/tabulate.hpp"


arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
OpenFileReader(int argc, char *argv[]);


tabulate::Table InitOutTable(const std::shared_ptr<arrow::Schema> &schema, int first_col_width = 3);
