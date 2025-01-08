/*
  Apache Arrow files CLI preview:
  write first rows of table, similar to pandas.DataFrame.head().
  
  This assumes that file contains only one table.
  The output is formatted as table with tabulate lib:
  https://github.com/p-ranav/tabulate.
  
  The columns width is equal among data columns and is adjusted automatically
  to fit screen entirely. However, in case of tables with a lot of columns,
  this may result in very narrow columns and bad readability.
  
  Takes filename as single CLI argument.
  Usage:
  arrow-head data.arrow 
*/

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <cmath>
#include <iostream>
#include <memory>

#include "lib/tabulate.hpp"
#include "utils.h"


// how many rows to show in arrow-head
const int64_t N_ROWS_DEFAULT = 5;


// write min(N_ROWS_DEFAULT, <number of rows>) rows to tabulate::Table
arrow::Status WriteContents(
    tabulate::Table &out_table,
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> &reader,
    int64_t n_rows_to_show) {
  // iterate over batches as rows can be distributes between them 
  for (int batch_idx = 0, n_rows_read = 0;
       batch_idx < reader->num_record_batches() && n_rows_read < n_rows_to_show;
       ++batch_idx) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(batch_idx));
    
    std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();
    // for each row in batch
    for (int row_idx = 0; n_rows_read < n_rows_to_show; ++n_rows_read) {
      tabulate::RowStream row_stream;
      row_stream << n_rows_read;
      // for each column in row
      for (auto col : columns) {
        ARROW_ASSIGN_OR_RAISE(auto scalar, col->GetScalar(row_idx));
        row_stream << scalar->ToString();
      }
      out_table.add_row(row_stream);
    }
  }

  return arrow::Status::OK();
}


// main execution function
arrow::Status ArrowHead(int argc, char *argv[]) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(argc, argv));
  tabulate::Table out_table = InitOutTable(reader->schema());
  // in case of table consisting less than N_ROWS_DEFAULT rows
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
