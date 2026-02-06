#include <filesystem>
#include <functional>
#include <string>

#include "../include/PatternCache.hpp"
#include "../include/FileIterator.hpp"
#include "../include/WorkerThreadsPool.hpp"

#define MIN_CHARS_PER_THREAD 10*MB
struct Pos {
  std::string::const_iterator beginIt;
  std::string::const_iterator endIt;
  std::string::const_iterator patternIt;
};

class Ssearch {
 public:
  inline int threadedSearchFile(const std::filesystem::path &path,
                   const std::string &pattern,
                   const std::function<void(Pos &&pos)> &action,
                   unsigned int threads = 1, int nchars = 256) {
    FileIterator reader{path};
    if (!reader.fileObjIsGood()) return FILE_READ_FAIL;

    auto workers = WorkerThreadsPool::makeSharedPtrTo(threads);
    if (!workers) return THREAD_SPAWN_FAIL;

#define DEFAULT_FNONSEQ                                             \
  [&pattern, &action, nchars, threads, workers,                     \
   this](const std::string &buf) mutable -> int {                   \
    threadedSearchText(buf, pattern, action, nchars, threads, workers); \
    return CONTINUE_ITERATION;                                                   \
  }
    int status = reader.iterWithFunctor(DEFAULT_FNONSEQ);
    workers->waitForQueueEmpty();
    return status;
  }

  inline int searchFile(const std::filesystem::path &path, const std::string &pattern,
                  const std::function<void(Pos &&pos)> &action,
                  int nchars = 256) {
    FileIterator reader{path};
    if (!reader.fileObjIsGood()) return FILE_READ_FAIL;

#define DEFAULT_FSEQ                                              \
  [&](const std::string &buf) -> int {                            \
    searchText(buf, pattern, buf.begin(), buf.end(), action, nchars); \
    return CONTINUE_ITERATION;                                                 \
  }

    return reader.iterWithFunctor(DEFAULT_FSEQ);
  }

  inline int threadedSearchText(
      const std::string &text, const std::string &pattern,
      const std::function<void(Pos &&pos)> &action, int nchars = 256,
      unsigned int threads = 1,
      std::shared_ptr<WorkerThreadsPool> threadPoolObj = NULL) {
    std::atomic<size_t> cnt = 0;
    const std::ptrdiff_t lap = pattern.length() - 1;

    std::shared_ptr<WorkerThreadsPool> workers;
    if (threadPoolObj)
      workers = threadPoolObj;
    else {
      workers = WorkerThreadsPool::makeSharedPtrTo(threads);
      if (!workers) return THREAD_SPAWN_FAIL;
    }

    for (std::string::const_iterator
             startPos = text.begin(),
             endPos = std::min(text.end(), text.begin() + MIN_CHARS_PER_THREAD + lap); endPos <= text.end()
         ; startPos += MIN_CHARS_PER_THREAD,
             endPos += MIN_CHARS_PER_THREAD + lap) {
      workers->pushTask([&, startPos, endPos]() {
        cnt.fetch_add(searchText(text, pattern, startPos, endPos, action, nchars));
      });
    }

    workers->waitForQueueEmpty();
    return cnt;
  }

  inline int searchText(const std::string &text, const std::string &pattern,
                    std::string::const_iterator startPos,
                    std::string::const_iterator endPos,
                    const std::function<void(Pos &&pos)> &action,
                    int nchars = 256) {
    size_t cnt = 0;
    const PatternCacheObj &data = page_.queryPatternCache(nchars, pattern);
    std::ptrdiff_t m = pattern.length(), n = text.length(),
                   s = startPos - text.begin(), en = endPos - text.begin(), j,
                   shift_gsfx, shift_bchr;
    while (s <= en - m) {
      j = m - 1;
      while (j >= 0 && pattern[j] == text[s + j]) --j;

      if (j < 0) {
        action(Pos{
            .beginIt = text.begin(), .endIt = text.end(), .patternIt = text.begin() + s});
        ++cnt;
        shift_bchr = (s + m < en) ? m - data.badchars[text[s + m]]
                                  : static_cast<std::ptrdiff_t>(1);
        shift_gsfx = data.shift[0];
      } else {
        shift_gsfx = data.shift[j + 1];
        shift_bchr = std::max(static_cast<std::ptrdiff_t>(1),
                              j - data.badchars[text[s + j]]);
      }

      s += std::max(shift_gsfx, shift_bchr);
    }
    return cnt;
  }

 private:
  PatternCacheResolvr page_;
};
