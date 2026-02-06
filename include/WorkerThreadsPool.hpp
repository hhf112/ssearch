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

  size_t getNumOfActiveThreads() { return threads_vec_.size(); }

  inline int trySpawnThreads(unsigned int n = 1) {
    kill_trigger_ = false;
    try {
      while (n--) {
        threads_vec_.emplace_back(std::thread([this]() -> void {
          active_thread_cnt_.fetch_add(1);
          std::function<void()> task;

          while (true) {
            std::unique_lock<std::mutex> take(task_queue_mutex_);
            cv_.wait(take,
                     [this]() { return kill_trigger_ || !task_queue_.empty(); });

            if (kill_trigger_) {
              active_thread_cnt_.fetch_sub(1);
              cv_.notify_all();
              return;
            } else {
              task = std::move(task_queue_.front());
              task_queue_.pop();
              take.unlock();

              working_thread_cnt_.fetch_add(1);

              task();

              working_thread_cnt_.fetch_sub(1);
              cv_.notify_all();
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
    std::lock_guard<std::mutex> guard(task_queue_mutex_);

    if constexpr (sizeof...(Args) != 0)
      task_queue_.push(std::bind_front(std::forward(fn), std::forward(args)...));
    else
      task_queue_.push(fn);

    cv_.notify_one();
  }

  static inline std::shared_ptr<WorkerThreadsPool> makeSharedPtrTo(int n = 0) {
    auto obj = std::make_shared<WorkerThreadsPool>();
    if (obj->trySpawnThreads(n) != OK) return nullptr;
    return obj;
  }

  inline void enableKillTrigger() {
    std::lock_guard<std::mutex> take(task_queue_mutex_);
    kill_trigger_.store(true);
    cv_.notify_all();
  }

  inline void waitForTasksComplete() {
    std::unique_lock<std::mutex> take(task_queue_mutex_);
    cv_.wait(take, [this]() { return working_thread_cnt_.load() == 0 && task_queue_.size() == 0; });
  }

  inline void forceClearQueue() {
    std::lock_guard<std::mutex> take(task_queue_mutex_);
    while (!task_queue_.empty()) task_queue_.pop();
  }

  inline void waitForTasksCompleteAndHarvest() {
    waitForTasksComplete();

    {
      std::lock_guard<std::mutex> take(task_queue_mutex_);
      kill_trigger_.store(true);
      cv_.notify_all();
    }

    std::unique_lock<std::mutex> take(task_queue_mutex_);
    cv_.wait(take, [this] { return active_thread_cnt_ == 0; });
    for (auto &worker : threads_vec_) worker.join();
  }

  ~WorkerThreadsPool() { waitForTasksCompleteAndHarvest(); }
  int32_t getWorkingThreadsCnt() { return working_thread_cnt_.load();}
  int32_t getActiveThreadsCnt() { return active_thread_cnt_.load();}
  int32_t getNumTasksRemaining() { 
    std::lock_guard<std::mutex> take (task_queue_mutex_);
    return task_queue_.size();
  }

 private:
  std::mutex task_queue_mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> task_queue_;
  std::vector<std::thread> threads_vec_;
  std::atomic<bool> kill_trigger_ = false;
  std::atomic<int32_t> working_thread_cnt_ = 0;
  std::atomic<int32_t> active_thread_cnt_ = 0;
};

