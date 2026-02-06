#include <atomic>
#include <chrono>

#include "./singlev2/Ssearch.hpp"

// 2688657
int main(int argc, char* argv[]) {
  Ssearch search;
  std::atomic<int> some = 0;
  auto start = std::chrono::high_resolution_clock::now();
  search.threadedSearchFile(argv[1], "example",
              [&some](Pos&& pos) -> void { ++some; }, 8);
  std::cerr << some << '\n';
  // std::string something = "this";
  // std::cerr << search.threadedSearchText(something, "this", [](Pos&&
  // Pos){std::cerr << "THIS" << '\n';}) <<'\n';
  auto end = std::chrono::high_resolution_clock::now();
  std::cerr << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                     start)
            << '\n';
  return 0;
}
