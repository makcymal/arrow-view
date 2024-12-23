#include "viewer.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>


arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
OpenFileReader(const std::string &arrow_file) {
  if (!std::filesystem::exists(arrow_file)) {
    std::cerr << "Arrow file \"" << arrow_file << "\" doesn't exist\n";
    exit(EXIT_FAILURE);
  }

  ARROW_ASSIGN_OR_RAISE(
    auto infile,
    arrow::io::ReadableFile::Open(arrow_file, arrow::default_memory_pool()));

  return arrow::ipc::RecordBatchFileReader::Open(infile);
}


arrow::Status ViewArrow(const RuntimeConfig &rc) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(rc.arrow_file));
  // ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
  
  for (int i = 0; i < reader->num_record_batches(); ++i) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(0));
    batch->
  }
}
