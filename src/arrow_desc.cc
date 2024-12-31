#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_aggregate.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/exec.h>
#include <arrow/result.h>
#include <arrow/scalar.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include <iostream>
#include <memory>
#include <string>

#include "lib/tabulate.hpp"
#include "utils.h"


enum DescStats {
  COUNT,
  MEAN,
  STD,
  MIN,
  QUAN25,
  QUAN50,
  QUAN75,
  MAX,
  ENUM_SIZE
};

std::string to_string(DescStats stat) {
  switch (stat) {
    case DescStats::COUNT:
      return "count";
    case DescStats::MEAN:
      return "mean";
    case DescStats::STD:
      return "std";
    case DescStats::MIN:
      return "min";
    case DescStats::QUAN25:
      return "25%";
    case DescStats::QUAN50:
      return "50%";
    case DescStats::QUAN75:
      return "75%";
    case DescStats::MAX:
      return "max";
    default:
      return "error";
  }
}


arrow::compute::RoundOptions ROUND_OPTS(3);
arrow::compute::QuantileOptions QUANTILE_OPTS({0.0, 0.25, 0.5, 0.75, 1.0});


arrow::Result<std::string> GetCount(
    const std::shared_ptr<arrow::ChunkedArray> &array) {
  return std::to_string(array->length() - array->null_count());
}

arrow::Result<std::string> GetMean(
    const std::shared_ptr<arrow::ChunkedArray> &array) {
  ARROW_ASSIGN_OR_RAISE(auto datum, arrow::compute::Mean(array));
  ARROW_ASSIGN_OR_RAISE(datum, arrow::compute::Round(datum, ROUND_OPTS));
  return datum.scalar()->ToString();
}

arrow::Result<std::string> GetStd(
    const std::shared_ptr<arrow::ChunkedArray> &array) {
  ARROW_ASSIGN_OR_RAISE(auto datum, arrow::compute::Stddev(array));
  ARROW_ASSIGN_OR_RAISE(datum, arrow::compute::Round(datum, ROUND_OPTS));
  return datum.scalar()->ToString();
}

arrow::Result<std::vector<std::string>> GetQuantiles(
    const std::shared_ptr<arrow::ChunkedArray> &array) {
  ARROW_ASSIGN_OR_RAISE(auto datum,
                        arrow::compute::Quantile(array, QUANTILE_OPTS));
  ARROW_ASSIGN_OR_RAISE(datum, arrow::compute::Round(datum, ROUND_OPTS));
  auto quantiles = datum.make_array();
  ARROW_ASSIGN_OR_RAISE(auto min, quantiles->GetScalar(0));
  ARROW_ASSIGN_OR_RAISE(auto quan25, quantiles->GetScalar(1));
  ARROW_ASSIGN_OR_RAISE(auto quan50, quantiles->GetScalar(2));
  ARROW_ASSIGN_OR_RAISE(auto quan75, quantiles->GetScalar(3));
  ARROW_ASSIGN_OR_RAISE(auto max, quantiles->GetScalar(4));

  return std::vector{min->ToString(), quan25->ToString(), quan50->ToString(),
                     quan75->ToString(), max->ToString()};
}


bool IsNumericType(const std::shared_ptr<arrow::DataType> &datatype) {
  switch (datatype->id()) {
    case arrow::Type::UINT8:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::INT16:
    case arrow::Type::UINT32:
    case arrow::Type::INT32:
    case arrow::Type::UINT64:
    case arrow::Type::INT64:
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::DECIMAL32:
    case arrow::Type::DECIMAL64:
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256:
      return true;
    default:
      return false;
  }
}


std::pair<std::vector<std::shared_ptr<arrow::Field>>, std::vector<int>>
ExtractNumericFields(const arrow::FieldVector &fields) {
  std::vector<std::shared_ptr<arrow::Field>> numer_fields;
  std::vector<int> numer_fields_idx;
  for (int idx = 0; idx < static_cast<int>(fields.size()); ++idx) {
    if (IsNumericType(fields[idx]->type())) {
      numer_fields.push_back(
          arrow::field(fields[idx]->name(), fields[idx]->type()));
      numer_fields_idx.push_back(idx);
    }
  }
  return {numer_fields, numer_fields_idx};
}


arrow::Status ArrowDesc(int argc, char *argv[]) {
  ARROW_ASSIGN_OR_RAISE(auto reader, OpenFileReader(argc, argv));
  auto fields = reader->schema()->fields();
  auto [numer_fields, numer_fields_idx] = ExtractNumericFields(fields);


  auto out_schema = arrow::schema(numer_fields);
  tabulate::Table out_table = InitOutTable(out_schema, 7);
  std::vector raw_out_table(numer_fields.size(),
                            std::vector<std::string>(DescStats::ENUM_SIZE));

  ARROW_ASSIGN_OR_RAISE(auto data_table, reader->ToTable());

  for (int i = 0; i < static_cast<int>(numer_fields.size()); ++i) {
    auto column = data_table->column(numer_fields_idx[i]);
    ARROW_ASSIGN_OR_RAISE(auto count, GetCount(column));
    ARROW_ASSIGN_OR_RAISE(auto mean, GetMean(column));
    ARROW_ASSIGN_OR_RAISE(auto std, GetStd(column));
    ARROW_ASSIGN_OR_RAISE(auto quantiles, GetQuantiles(column));

    raw_out_table[i][COUNT] = count;
    raw_out_table[i][MEAN] = mean;
    raw_out_table[i][STD] = std;
    raw_out_table[i][MIN] = quantiles[0];
    raw_out_table[i][QUAN25] = quantiles[1];
    raw_out_table[i][QUAN50] = quantiles[2];
    raw_out_table[i][QUAN75] = quantiles[3];
    raw_out_table[i][MAX] = quantiles[4];
  }

  for (int stat_idx = 0; stat_idx < DescStats::ENUM_SIZE; ++stat_idx) {
    tabulate::RowStream out_row;
    out_row << to_string(DescStats(stat_idx));
    for (int i = 0; i < static_cast<int>(numer_fields.size()); ++i) {
      out_row << std::string(raw_out_table[i][stat_idx]);
    }
    out_table.add_row(out_row);
  }

  std::cout << out_table << std::endl;

  return arrow::Status::OK();
}


int main(int argc, char *argv[]) {
  arrow::Status st = ArrowDesc(argc, argv);
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
