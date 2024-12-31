#include "utils.h"

#include <arrow/io/api.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>


arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchFileReader>>
OpenFileReader(int argc, char *argv[]) {
  if (argc == 1) {
    std::cerr << "Pass the name of Arrow file\n";
    exit(EXIT_FAILURE);
  } else if (argc > 2) {
    std::cerr << "Pass only one name of Arrow file\n";
    exit(EXIT_FAILURE);
  }
  std::string arrow_file{argv[1]};

  if (!std::filesystem::exists(arrow_file)) {
    std::cerr << "Arrow file \"" << arrow_file << "\" doesn't exist\n";
    exit(EXIT_FAILURE);
  }

  ARROW_ASSIGN_OR_RAISE(
      auto infile,
      arrow::io::ReadableFile::Open(arrow_file, arrow::default_memory_pool()));

  return arrow::ipc::RecordBatchFileReader::Open(infile);
}


#ifdef WIN32  // on windows

#include <windows.h>

int GetConsoleWidth() {
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  int columns;
  GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
  return csbi.srWindow.Right - csbi.srWindow.Left + 1;
}

#else  // on linux

#include <sys/ioctl.h>
#include <unistd.h>

int GetConsoleWidth() {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
  return w.ws_col;
}

#endif


tabulate::Table InitOutTable(const std::shared_ptr<arrow::Schema> &schema, int first_col_width) {
  arrow::FieldVector fields = schema->fields();
  tabulate::RowStream header_stream;
  header_stream << "#";
  for (auto fld : fields) {
    header_stream << fld->name();
  }
  tabulate::Table table;
  table.add_row(header_stream);

  int n_fields = fields.size();
  int field_width = (GetConsoleWidth() - first_col_width - n_fields - 1) / n_fields;

  table.column(0).format().width(first_col_width);
  for (int i = 1; i <= n_fields; ++i) {
    table.column(i).format().width(field_width);
  }

  return table;
}
