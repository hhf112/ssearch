#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#define THREAD_SPAWN_FAIL 2
#define OK 0

class WorkerThreadsPool {
 public:
  WorkerThreadsPool() = default;
  WorkerThreadsPool(const WorkerThreadsPool &workers) = delete;
  WorkerThreadsPool &operator=(const WorkerThreadsPool &workers) = delete;

  inline int trySpawnThreads(unsigned int n = 1) {
    killTrigger_ = false;
    try {
      while (n--) {
        threadsVec_.emplace_back(std::thread([this]() -> void {
          std::function<void()> pushTask;
          while (true) {
            std::unique_lock<std::mutex> take(taskVecMutex_);
            cv_.wait(take,
                     [this]() { return killTrigger_ || !taskVec_.empty(); });
            if (killTrigger_) {
              return;
            } else {
              pushTask = std::move(taskVec_.front());
              taskVec_.pop();
              cv_.notify_all();
              pushTask();
            }
          }
        }));
      }

    } catch (std::system_error &e) {
      return THREAD_SPAWN_FAIL;
    }

    return OK;
  }

  template <typename Functor, typename... Args>
  inline void pushTask(Functor &&fn, Args &&...args) {
    std::lock_guard<std::mutex> guard(taskVecMutex_);
    if constexpr (sizeof...(Args) != 0)
      taskVec_.push(std::bind_front(std::forward(fn), std::forward(args)...));
    else
      taskVec_.push(fn);
    cv_.notify_one();
  }

  static inline std::shared_ptr<WorkerThreadsPool> makeSharedPtrTo(int n = 0) {
    auto obj = std::make_shared<WorkerThreadsPool>();
    if (obj->trySpawnThreads(n) != OK) return nullptr;
    return obj;
  }

  inline void enableKillTrigger() {
    std::lock_guard<std::mutex> take(taskVecMutex_);
    killTrigger_ = true;
    cv_.notify_all();
  }

  inline void waitForQueueEmpty() {
    std::unique_lock<std::mutex> take(taskVecMutex_);
    cv_.wait(take, [this]() { return taskVec_.empty(); });
  }

  inline void forceClearQueue() {
    std::lock_guard<std::mutex> take(taskVecMutex_);
    while (!taskVec_.empty()) taskVec_.pop();
  }

  inline void waitForQueueEmptyAndHarvest() {
    std::unique_lock<std::mutex> take(taskVecMutex_);
    cv_.wait(take, [this]() { return taskVec_.empty() || killTrigger_; });
    if (!killTrigger_) {
      killTrigger_ = true;
      cv_.notify_all();
    }
    take.unlock();
    for (auto &worker : threadsVec_) worker.join();
  }

  ~WorkerThreadsPool() { waitForQueueEmptyAndHarvest(); }

 private:
  std::mutex taskVecMutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> taskVec_;
  std::vector<std::thread> threadsVec_;
  std::atomic<bool> killTrigger_ = false;
};

#define MB 1048576
#define CONTINUE_ITERATION 0
#define INVALID_OVERLAP_LENGTH -1
#define FILE_READ_FAIL -2

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

struct PatternCacheObj {
  std::vector<size_t> shift, bpos;
  std::vector<ptrdiff_t> badchars;
  int NCHARS;

  PatternCacheObj() = default;
  PatternCacheObj(int nchars, size_t patternlen) : NCHARS{nchars} {
    shift.resize(patternlen + 1);
    bpos.resize(patternlen + 1);
    badchars.resize(nchars, -1);
  }
};

class PatternCacheResolvr {
 public:
  PatternCacheResolvr() = default;

  /*@brief fetch from or create in preprocessed pattern PatternCacheObj*/
  inline const PatternCacheObj &queryPatternCache(int nchars,
                                                  const std::string &str) {
    if (page_.count(str)) return page_[str];

    int n = str.length();
    PatternCacheObj data(nchars, str.length());
    badchh(data.badchars, str, n);
    ssuffix(data.shift, data.bpos, str, n);
    case1(data.shift, data.bpos, str, n);

    auto [newData, _] = page_.emplace(str, std::move(data));
    return newData->second;
  }

 private:
  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-for-pattern-searchTexting*/
  inline void badchh(std::vector<ptrdiff_t> &badhcars, const std::string &str,
                     size_t size) {
    size_t i;
    for (i = 0; i < size; i++) badhcars[(int)str[i]] = i;
  }

  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
  inline void ssuffix(std::vector<size_t> &shift, std::vector<size_t> &bpos,
                      const std::string &pat, size_t m) {
    size_t i, j;
    j = bpos[0];
    for (i = 0; i <= m; i++) {
      if (shift[i] == m) shift[i] = j;
      if (i == j) j = bpos[j];
    }
  }

  /*@src
   * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
  inline void case1(std::vector<size_t> &shift, std::vector<size_t> &bpos,
                    const std::string &pat, size_t m) {
    ptrdiff_t i = static_cast<ptrdiff_t>(m), j = static_cast<ptrdiff_t>(m + 1);
    bpos[i] = j;
    while (i > 0) {
      while (j <= m && pat[i - 1] != pat[j - 1]) {
        if (shift[j] == m) shift[j] = j - i;
        j = bpos[j];
      }
      i--;
      j--;
      bpos[i] = j;
    }
  }

  std::map<std::string, PatternCacheObj> page_;
};

#define MIN_CHARS_PER_THREAD 10 * MB
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

#define DEFAULT_FNONSEQ                                                 \
  [&pattern, &action, nchars, threads, workers,                         \
   this](const std::string &buf) mutable -> int {                       \
    threadedSearchText(buf, pattern, action, nchars, threads, workers); \
    return CONTINUE_ITERATION;                                          \
  }
    int status = reader.iterWithFunctor(DEFAULT_FNONSEQ);
    return status;
  }

  inline int searchFile(const std::filesystem::path &path,
                        const std::string &pattern,
                        const std::function<void(Pos &&pos)> &action,
                        int nchars = 256) {
    FileIterator reader{path};
    if (!reader.fileObjIsGood()) return FILE_READ_FAIL;

#define DEFAULT_FSEQ                                                  \
  [&](const std::string &buf) -> int {                                \
    searchText(buf, pattern, buf.begin(), buf.end(), action, nchars); \
    return CONTINUE_ITERATION;                                        \
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
             endPos = std::min(text.end(),
                               text.begin() + MIN_CHARS_PER_THREAD + lap);
         endPos <= text.end(); startPos += MIN_CHARS_PER_THREAD,
             endPos += MIN_CHARS_PER_THREAD + lap) {
      workers->pushTask([&, startPos, endPos]() {
        cnt.fetch_add(
            searchText(text, pattern, startPos, endPos, action, nchars));
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
