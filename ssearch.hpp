#include <climits>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace util {
#define SYS_ERR -1
#define OK 0
#define DEBUG(str) std::cerr << "[DEBUG] " << str;
class BlockingThreadPool {
  public:
	BlockingThreadPool() = default;
	BlockingThreadPool(const BlockingThreadPool &workers) = delete;
	BlockingThreadPool &operator=(const BlockingThreadPool &workers) = delete;

	inline int trySpawnThreads(unsigned int threads_cnt = 1) {
		kill_all_threads_ = false;
		try {
			while (threads_cnt-- > 0) {
				threads_vec_.emplace_back(std::thread([this]() -> void {
					active_thread_cnt_.fetch_add(1);
					joinable_thread_cnt_.fetch_add(1);

					std::function<void()> task;

					while (true) {
						std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);
						task_available_.wait(task_q_lock, [this]() {
							return kill_all_threads_ || !task_q_.empty();
						});

						if (kill_all_threads_) {
							active_thread_cnt_.fetch_sub(1);
							alive_threads_.notify_all();
							return;
						} else {
							task = std::move(task_q_.front());
							task_q_.pop();
							task_q_lock.unlock();

							working_threads_cnt_.fetch_add(1);

							task();

							working_threads_cnt_.fetch_sub(1);
							ongoing_work_.notify_all();
						}
					}
				}));
			}
			return OK;
		} catch (std::system_error &e) {
			return SYS_ERR;
		}
	}

	template <typename Functor, typename... Args>
	inline void pushTask(Functor &&fn, Args &&...args) {
		{
			std::lock_guard<std::mutex> task_q_lock(task_q_mtx_);

			if constexpr (sizeof...(Args) != 0)
				task_q_.push(std::bind_front(std::forward<Functor>(fn),
											 std::forward<Args>(args)...));
			else
				task_q_.push(fn);
		}

		task_available_.notify_one();
	}

	static inline std::shared_ptr<BlockingThreadPool>
	makeSharedPtrTo(int n = 0) {
		auto obj = std::make_shared<BlockingThreadPool>();
		if (obj->trySpawnThreads(n) != OK)
			return nullptr;
		return obj;
	}

	inline void forceClearQueue() {
		std::lock_guard<std::mutex> take(task_q_mtx_);
		while (!task_q_.empty())
			task_q_.pop();
		// DEBUG("forceClearQueue(): queue emptied\n")
	}

	inline void waitForTasksComplete() {
		std::unique_lock<std::mutex> take(task_q_mtx_);
		ongoing_work_.wait(take, [this]() {
			// DEBUG("waitForTasksComplete(): checking. working_threads_cnt_ = " << working_threads_cnt_
			// 																  << " queue size: " << task_q_.size() << '\n');
			return working_threads_cnt_ == 0 && task_q_.size() == 0;
		});

		// DEBUG("waitForTasksComplete(): queue emptied and working threads = 0\n")
	}

	inline int waitForTasksCompleteAndHarvestThreads() {
		try {
			std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);

			DEBUG("waitForTasksCompleteAndHarvestThreads(): waiting for working_thread_cnt_ and task_q.size()\n");
			ongoing_work_.wait(task_q_lock, [this]() {
				return working_threads_cnt_ == 0 && task_q_.size() == 0;
			});

			DEBUG("waitForTasksCompleteAndHarvestThreads(): working threads = 0, task q empty.\n");

			kill_all_threads_.store(true);
			task_available_.notify_all();

			alive_threads_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });

			DEBUG("waitForTasksCompleteAndHarvestThreads(): active_thread_cnt = 0\n");

			for (auto &worker : threads_vec_) {
				if (worker.joinable()) {
					worker.join();
					joinable_thread_cnt_.fetch_sub(1);
				}
			}
			threads_vec_.clear();
			// DEBUG("forceterminateThreads(): threads harvested\n");

			kill_all_threads_.store(false);
			return OK;
		} catch (std::system_error &e) {
			return SYS_ERR;
		}
	}

	inline int forceTerminateThreads() {
		try {
			std::unique_lock<std::mutex> task_q_lock(task_q_mtx_);

			kill_all_threads_.store(true);
			task_available_.notify_all();

			alive_threads_.wait(task_q_lock, [this] { return active_thread_cnt_ == 0; });
			// DEBUG("forceterminateThreads(): wait completed, " << active_thread_cnt_ << " active threads.\n must join " << threads_vec_.size() << " threads.\n");

			for (auto &worker : threads_vec_) {
				if (worker.joinable()) {
					// DEBUG("forceterminateThreads(): " << "joining a thread.\n");
					worker.join();
					joinable_thread_cnt_.fetch_sub(1);
					// DEBUG("forceterminateThreads(): " << joinable_thread_cnt_ << " threads remaining to join.\n");
				} // else DEBUG("forceterminateThreads(): " << "encountered an unjoinable thread.\n");
			}
			threads_vec_.clear();
			// DEBUG("forceterminateThreads(): threads harvested\n");

			kill_all_threads_.store(false);
			return OK;
		} catch (std::system_error &e) {
			return SYS_ERR;
		}
	}

	~BlockingThreadPool() {
		// DEBUG("~BlockingThreadPool(): desstructor initiaited\n");
		if (joinable_thread_cnt_ > 0)
			forceTerminateThreads();
		// DEBUG("~BlockingThreadPool(): threads harvested\n");
		if (task_q_.size() > 0)
			forceClearQueue();
		// DEBUG("~BlockingThreadPool(): task queue empty\n");
	}

	int32_t getWorkingThreadsCnt() { return working_threads_cnt_.load(); }
	int32_t getActiveThreadsCnt() { return active_thread_cnt_.load(); }
	int32_t getJoinableThreadsCnt() { return joinable_thread_cnt_.load(); }
	std::function<void()> popTaskQueue() {
		std::lock_guard<std::mutex> guard(task_q_mtx_);
		return task_q_.front();
	}
	int32_t getTaskQueueSize() {
		std::lock_guard<std::mutex> take(task_q_mtx_);
		return task_q_.size();
	}

  private:
	std::mutex task_q_mtx_;
	std::condition_variable cv_;
	std::condition_variable task_available_;
	std::condition_variable ongoing_work_;
	std::condition_variable alive_threads_;
	std::queue<std::function<void()>> task_q_;
	std::vector<std::thread> threads_vec_;
	std::atomic<bool> kill_all_threads_ = false;
	std::atomic<int32_t> working_threads_cnt_ = 0;
	std::atomic<int32_t> active_thread_cnt_ = 0;
	std::atomic<int32_t> joinable_thread_cnt_ = 0;
};

class FileChunker {
#define READ_NEXT 0
#define INVALID_OVERLAP -1
#define READ_FAIL 2

  public:
	FileChunker() = default;

	uint8_t runCallbackPerChunk(
		std::string_view path,
		std::function<uint8_t(const std::list<std::string>::iterator it)>
			&&callback,
		const size_t chunk_size, const size_t overlap) {
		if (overlap >= chunk_size)
			return INVALID_OVERLAP;

		std::fstream file_obj(path.data(), std::ios_base::in);

		if (!file_obj.good()) {
			return READ_FAIL;
		}

		while (file_obj.good()) {
			buf_list_.insert(buf_list_.begin(), std::string());
			auto buf_it = buf_list_.begin();
			buf_it->resize(chunk_size + overlap);

			file_obj.read(buf_it->data(), buf_it->length());

			if (callback(buf_it) != READ_NEXT)
				break;

			if (file_obj.eof()) {
				break;
			} else if (!file_obj.good()) {
				return READ_FAIL;
			}

			file_obj.seekg(-overlap, std::ios_base::cur);
		}

		return OK;
	}

	void eraseBuf(const std::list<std::string>::iterator it) {
		buf_list_.erase(it);
	}

	void clearBufList() { buf_list_.clear(); }

  private:
	std::list<std::string> buf_list_;
};
} // namespace util

namespace ssearch {
struct PatternData {
	std::vector<size_t> shift, bpos;
	std::vector<ptrdiff_t> badchars;
	int num_chars;

	PatternData() = default;
	PatternData(int nchars, size_t pat_len) : num_chars{nchars} {
		shift.resize(pat_len + 1);
		bpos.resize(pat_len + 1);
		badchars.resize(nchars, -1);
	}
};

class PatternDataFactory {
  public:
	PatternDataFactory() = default;

	inline const PatternData &getPatternData(int num_chars,
											 const std::string_view str) {
		if (data_map_.count(str))
			return data_map_[str];

		int n = str.length();
		PatternData data(num_chars, str.length());
		fillBadChar(data.badchars, str);
		strongSuffix(data.shift, data.bpos, str, n);
		specialCase(data.shift, data.bpos, str, n);

		auto [newData, _] = data_map_.emplace(str, std::move(data));
		return newData->second;
	}

  private:
	inline void fillBadChar(std::vector<ptrdiff_t> &badhcars,
							const std::string_view str) {
		size_t max_idx = str.size();
		for (size_t char_idx = 0; char_idx < max_idx; char_idx++)
			badhcars[(int8_t)str[char_idx]] = char_idx;
	}

	/*@src
	 * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
	inline void strongSuffix(std::vector<size_t> &shift,
							 std::vector<size_t> &bpos,
							 const std::string_view pat, size_t m) {
		size_t i, j;
		j = bpos[0];
		for (i = 0; i <= m; i++) {
			if (shift[i] == m)
				shift[i] = j;
			if (i == j)
				j = bpos[j];
		}
	}

	/*@src
	 * https://www.geeksforgeeks.org/dsa/boyer-moore-algorithm-good-suffix-heuristic*/
	inline void specialCase(std::vector<size_t> &shift,
							std::vector<size_t> &bpos,
							const std::string_view pat, size_t m) {
		ptrdiff_t i = static_cast<ptrdiff_t>(m),
				  j = static_cast<ptrdiff_t>(m + 1);
		bpos[i] = j;
		while (i > 0) {
			while (j <= m && pat[i - 1] != pat[j - 1]) {
				if (shift[j] == m)
					shift[j] = j - i;
				j = bpos[j];
			}
			i--;
			j--;
			bpos[i] = j;
		}
	}

	std::map<const std::string_view, PatternData> data_map_;
};

struct Pos {
	std::string::const_iterator begin;
	std::string::const_iterator end;
	std::string::const_iterator pattern;
};

class SE {
  public:
	inline int threadedSearchFile(
		const std::string_view path, const std::string_view pattern,
		const std::function<void(Pos &&pos)> &callback,
		unsigned int cnt_threads, size_t chunk_size, int num_chars = 256) {
		auto workers = util::BlockingThreadPool::makeSharedPtrTo(cnt_threads);
		if (!workers)
			return SYS_ERR;

		util::FileChunker reader;
		int status = reader.runCallbackPerChunk(
			path,
			[pattern, &callback, num_chars, cnt_threads, workers, &reader,
			 this](
				const std::list<std::string>::iterator buf_it) mutable -> int {
				workers->pushTask([pattern, &callback, num_chars, cnt_threads,
								   workers, &reader, buf_it, this]() mutable {
					searchText(buf_it->begin(), buf_it->end(), pattern,
							   callback, num_chars);
					reader.eraseBuf(buf_it);
				});
				return READ_NEXT;
			},
			chunk_size, pattern.length() - 1);

		workers->waitForTasksComplete();
		return status;
	}

	inline int searchFile(const std::string_view path,
						  const std::string_view pattern,
						  const std::function<void(Pos &&pos)> &callback,
						  size_t chunk_size, int nchars = 256) {
		util::FileChunker reader;
		uint8_t status = reader.runCallbackPerChunk(
			path,
			[&](const std::list<std::string>::iterator buf) -> int {
				searchText(buf->begin(), buf->end(), pattern, callback, nchars);
		reader.clearBufList();
				return READ_NEXT;
			},
			chunk_size, pattern.length() - 1);
		return status;
	}

	inline int threadedSearchText(
		const std::string::const_iterator begin,
		std::string::const_iterator end, const std::string_view pattern,
		const std::function<void(Pos &&pos)> &callback,
		unsigned int cnt_threads, size_t partition,
		std::shared_ptr<util::BlockingThreadPool> thread_pool = NULL,
		int num_chars = 256) {
		std::atomic<size_t> cnt = 0;
		const std::ptrdiff_t overlap = pattern.length() - 1;

		std::shared_ptr<util::BlockingThreadPool> workers;
		if (thread_pool)
			workers = thread_pool;
		else {
			workers = util::BlockingThreadPool::makeSharedPtrTo(cnt_threads);
			if (!workers)
				return SYS_ERR;
		}

		for (std::string::const_iterator
				 first = begin,
				 second = std::min(end, begin + partition + overlap);
			 second <= end; first += partition, second += partition + overlap) {
			workers->pushTask([&, first, second]() {
				cnt.fetch_add(
					searchText(first, second, pattern, callback, num_chars));
			});
		}

		workers->waitForTasksComplete();
		return cnt;
	}

	inline int searchText(std::string::const_iterator begin,
						  std::string::const_iterator end,
						  const std::string_view pattern,
						  const std::function<void(Pos &&pos)> &action,
						  int num_chars = 256) {
		size_t cnt = 0;
		const PatternData &data =
			pat_data_factory_.getPatternData(num_chars, pattern);

		std::ptrdiff_t m = pattern.length(), n = end - begin, j, shift_gsfx,
					   shift_bchr;

		if (n < m || m == 0) {
			return 0;
		}

		std::string::const_iterator s = begin, en = end;
		while (s <= (en - m)) {
			j = m - 1;

			while (j >= 0 && pattern[j] == *(s + j))
				--j;

			if (j < 0) {
				action(Pos{.begin = begin, .end = end, .pattern = s});
				++cnt;
				shift_bchr = (s + m < en) ? m - data.badchars[*(s + m)]
										  : static_cast<std::ptrdiff_t>(1);
				shift_gsfx = data.shift[0];
			} else {
				shift_gsfx = data.shift[j + 1];

				shift_bchr = std::max(static_cast<std::ptrdiff_t>(1),
									  j - data.badchars[*(s + j)]);
			}

			s += std::max(shift_gsfx, shift_bchr);
		}
		return cnt;
	}

  private:
	PatternDataFactory pat_data_factory_;
};
} // namespace ssearch
