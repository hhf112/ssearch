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
    kill_trigger_ = false;
    try {
      while (n--) {
        threads_vec_.emplace_back(std::thread([this]() -> void {
          std::function<void()> task;

          active_thread_cnt_.fetch_add(1);
          while (true) {
            std::unique_lock<std::mutex> take(task_vec_mutex_);
            cv_.wait(take,
                     [this]() { return kill_trigger_ || !task_vec_.empty(); });

            if (kill_trigger_) {
              active_thread_cnt_.fetch_sub(1);
              cv_.notify_all();
              return;
            } else {
              task = std::move(task_vec_.front());
              task_vec_.pop();
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
    std::lock_guard<std::mutex> guard(task_vec_mutex_);

    if constexpr (sizeof...(Args) != 0)
      task_vec_.push(std::bind_front(std::forward(fn), std::forward(args)...));
    else
      task_vec_.push(fn);

    cv_.notify_one();
  }

  static inline std::shared_ptr<WorkerThreadsPool> makeSharedPtrTo(int n = 0) {
    auto obj = std::make_shared<WorkerThreadsPool>();
    if (obj->trySpawnThreads(n) != OK) return nullptr;
    return obj;
  }

  inline void enableKillTrigger() {
    std::lock_guard<std::mutex> take(task_vec_mutex_);
    kill_trigger_.store(true);
    cv_.notify_all();
  }

  inline void waitForTasksComplete() {
    std::unique_lock<std::mutex> take(task_vec_mutex_);
    cv_.wait(take, [this]() { return working_thread_cnt_.load() == 0; });
  }

  inline void forceClearQueue() {
    std::lock_guard<std::mutex> take(task_vec_mutex_);
    while (!task_vec_.empty()) task_vec_.pop();
  }

  inline void waitForTasksCompleteAndHarvest() {
    // std::unique_lock<std::mutex> take(task_vec_mutex_);
    // cv_.wait(take, [this]() { return task_vec_.empty() || kill_trigger_; });
    // if (!kill_trigger_) {
    //   kill_trigger_ = true;
    //   cv_.notify_all();
    // }
    // take.unlock();
    waitForTasksComplete();

    {
      std::lock_guard<std::mutex> take(task_vec_mutex_);
      kill_trigger_.store(true);
      cv_.notify_all();
    }

    std::unique_lock<std::mutex> take(task_vec_mutex_);
    cv_.wait(take, [this] { return active_thread_cnt_ == 0; });
    for (auto &worker : threads_vec_) worker.join();
  }

  size_t getNumOfActiveThreads() {
    return threads_vec_.size();
  }

  ~WorkerThreadsPool() { waitForTasksCompleteAndHarvest(); }

 private:
  std::mutex task_vec_mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> task_vec_;
  std::vector<std::thread> threads_vec_;
  std::atomic<bool> kill_trigger_ = false;
  std::atomic<int32_t> working_thread_cnt_ = 0;
  std::atomic<int32_t> active_thread_cnt_ = 0;
};
