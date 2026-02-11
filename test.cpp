#include <atomic>
#include <chrono>

#include "./singlev2/Ssearch.hpp"

// 2688657
int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "./test <file> <pattern>\n";
    return 1;
  }

  ssearch::SE search;
  std::atomic<int> numSearched = 0;
  auto start = std::chrono::high_resolution_clock::now();

  search.searchFile(
      argv[1], argv[2],
      [&numSearched](ssearch::Pos&& pos) -> void { ++numSearched; });

  std::cerr << "sequential search found: " << numSearched
            << ", sequential search time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
            << '\n';

  numSearched.store(0);
  start = std::chrono::high_resolution_clock::now();

  search.threadedSearchFile(
      argv[1], "example",
      [&numSearched](ssearch::Pos&& pos) -> void { ++numSearched; }, 8);

  std::cerr << "threaded search found: " << numSearched
            << ", threaded search time: "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::high_resolution_clock::now() - start)
            << '\n';

  return 0;
}
