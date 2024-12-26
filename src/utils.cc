#include "utils.h"

#include <arrow/io/api.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>


std::string ParseArgs(int argc, char *argv[]) {
  if (argc == 1) {
    std::cerr << "Pass the name of Arrow file\n";
    exit(EXIT_FAILURE);
  } else if (argc > 2) {
    std::cerr << "Pass only one name of Arrow file\n";
    exit(EXIT_FAILURE);
  }
  return {argv[1]};
}


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


#ifdef WIN32
// on windows

#include <windows.h>

int GetConsoleWidth() {
  CONSOLE_SCREEN_BUFFER_INFO csbi;
  int columns;
  GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
  return csbi.srWindow.Right - csbi.srWindow.Left + 1;
}

#else
// on linux

#include <sys/ioctl.h>
#include <unistd.h>

int GetConsoleWidth() {
  struct winsize w;
  ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
  return w.ws_col;
}

#endif
