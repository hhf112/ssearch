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
    fileObject_ = std::fstream{path, std::ios::in};
    goodFlag_ = fileObject_.good();
  }

  int iterWithFunctor(std::function<int(const std::string &)> &&action,
                  size_t sz = 20 * MB, size_t lap = 0) {
    if (lap > sz) return INVALID_OVERLAP_LENGTH;
    bufferString_.resize(sz + lap);

    do {
      try {
        fileObject_.read(bufferString_.data(), sz + lap);
      } catch (std::ios_base::failure &f) {
        return FILE_READ_FAIL;
      }
      if (action(bufferString_) != CONTINUE_ITERATION) break;
      fileObject_.seekg(-lap, std::ios_base::cur);
    } while (fileObject_.gcount());

    goodFlag_ = fileObject_.good();
    return OK;
  }

  bool fileObjIsGood() { return goodFlag_; }

 private:
  std::fstream fileObject_;
  std::string bufferString_;
  bool goodFlag_ = false;
};
