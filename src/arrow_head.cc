#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <cmath>
#include <iostream>
#include <memory>

#include "lib/tabulate.hpp"
#include "utils.h"


const int64_t N_ROWS_DEFAULT = 5;


arrow::Status WriteContents(
    tabulate::Table &out_table,
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> &reader,
    int64_t n_rows_to_show) {
  for (int batch_idx = 0, n_rows_read = 0;
       batch_idx < reader->num_record_batches() && n_rows_read < n_rows_to_show;
       ++batch_idx) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(batch_idx));

    std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();
    for (int row_idx = 0; n_rows_read < n_rows_to_show; ++n_rows_read) {
      tabulate::RowStream row_stream;
      row_stream << n_rows_read;
      for (auto col : columns) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, col->GetScalar(row_idx));
        row_stream << scalar->ToString();
      }
      out_table.add_row(row_stream);
    }
  }

  return arrow::Status::OK();
}


arrow::Status ArrowHead(int argc, char *argv[]) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(argc, argv));
  tabulate::Table out_table = InitOutTable(reader->schema());
  ARROW_ASSIGN_OR_RAISE(auto n_rows_total, reader->CountRows());
  int64_t n_rows_to_show = std::min(N_ROWS_DEFAULT, n_rows_total);
  ARROW_RETURN_NOT_OK(WriteContents(out_table, reader, n_rows_to_show));
  std::cout << out_table;
  return arrow::Status::OK();
}


int main(int argc, char *argv[]) {
  arrow::Status st = ArrowHead(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
