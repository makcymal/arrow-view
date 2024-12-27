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


tabulate::Table InitTable(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> &reader) {
  arrow::FieldVector fields = reader->schema()->fields();
  tabulate::RowStream header_stream;
  header_stream << "#";
  for (auto fld : fields) {
    header_stream << fld->name();
  }

  tabulate::Table table;
  table.add_row(header_stream);

  int n_fields = fields.size();
  int field_width = (GetConsoleWidth() - 3 - n_fields - 1) / n_fields;

  table.column(0).format().width(3);
  for (int i = 1; i <= n_fields; ++i) {
    table.column(i).format().width(field_width);
  }

  return table;
}


arrow::Status WriteContents(
    tabulate::Table &table,
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
      table.add_row(row_stream);
    }
  }

  return arrow::Status::OK();
}


arrow::Status ArrowHead(const std::string &arrow_file) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(arrow_file));
  tabulate::Table table = InitTable(reader);
  ARROW_ASSIGN_OR_RAISE(auto n_rows_total, reader->CountRows());
  int64_t n_rows_to_show = std::min(N_ROWS_DEFAULT, n_rows_total);
  ARROW_RETURN_NOT_OK(WriteContents(table, reader, n_rows_to_show));
  std::cout << table;
  return arrow::Status::OK();
}


int main(int argc, char *argv[]) {
  std::string arrow_file = ParseArgs(argc, argv);
  arrow::Status st = ArrowHead(arrow_file);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
