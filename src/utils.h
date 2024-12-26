#include <arrow/api.h>
#include <arrow/ipc/api.h>

#include <string>


std::string ParseArgs(int argc, char *argv[]);

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
OpenFileReader(const std::string &arrow_file);

int GetConsoleWidth();
