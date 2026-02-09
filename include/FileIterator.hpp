#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <string>

#define MB 1048576
#define CONTINUE_ITERATION 0
#define INVALID_OVERLAP_LENGTH -1
#define FILE_READ_FAIL -2
#define OK 2

class FileIterator {
 public:
  FileIterator(std::filesystem::path path) {
    file_object_ = std::fstream{path, std::ios::in};
    good_flag_ = file_object_.good();
  }

  int iterWithFunctor(std::function<int(const std::string &)> &&action,
                  size_t sz = 20 * MB, size_t lap = 0) {
    if (lap > sz) return INVALID_OVERLAP_LENGTH;
    buffer_string_.resize(sz + lap);

    do {
      try {
        file_object_.read(buffer_string_.data(), sz + lap);
      } catch (std::ios_base::failure &f) {
        return FILE_READ_FAIL;
      }
      if (action(buffer_string_) != CONTINUE_ITERATION) break;
      file_object_.seekg(-lap, std::ios_base::cur);
    } while (file_object_.gcount());

    good_flag_ = file_object_.good();
    return OK;
  }

  bool fileObjIsGood() { return good_flag_; }

 private:
  std::fstream file_object_;
  std::string buffer_string_;
  bool good_flag_ = false;
};
