#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

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
