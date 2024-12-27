#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <cmath>
#include <iostream>
#include <memory>

#include "lib/tabulate.hpp"
#include "utils.h"


void FormatTable(tabulate::Table &table) {
  table.format()
      .hide_border_top()
      .hide_border_bottom()
      .border_left(" ")
      .border_right(" ")
      .corner(" ");
  table.row(1).format().show_border_top();
}


arrow::Result<std::vector<int64_t>> CountNulls(
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> &reader,
    const arrow::FieldVector &fields) {
  std::vector<int64_t> null_counts(fields.size(), 0);
  int n_batches = reader->num_record_batches();
  
  for (int batch_idx = 0; batch_idx < n_batches; ++batch_idx) {
    ARROW_ASSIGN_OR_RAISE(auto batch, reader->ReadRecordBatch(batch_idx));
    for (int fld_idx = 0; fld_idx < static_cast<int>(fields.size()); ++fld_idx) {
      std::shared_ptr<arrow::Array> col = batch->column(fld_idx);
      null_counts[fld_idx] += col->null_count();
    }
  }
  return null_counts;
}


arrow::Status ArrowInfo(const std::string &arrow_file) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(arrow_file));
  arrow::FieldVector fields = reader->schema()->fields();
  ARROW_ASSIGN_OR_RAISE(auto null_counts, CountNulls(reader, fields));

  tabulate::Table table;
  table.add_row({"#", "Field", "Non-Null Count", "Dtype"});

  for (auto fld_idx = 0; fld_idx < static_cast<int>(fields.size()); ++fld_idx) {
    table.add_row(tabulate::RowStream{} << fld_idx << fields[fld_idx]->name()
                                        << null_counts[fld_idx]
                                        << *fields[fld_idx]->type());
  }

  FormatTable(table);
  std::cout << table;

  return arrow::Status::OK();
}


int main(int argc, char *argv[]) {
  std::string arrow_file = ParseArgs(argc, argv);
  arrow::Status st = ArrowInfo(arrow_file);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
