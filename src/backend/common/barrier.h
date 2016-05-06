
#pragma once

#include <cassert>
#include <memory>
#include <vector>
#include <utility>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>

#include "backend/executor/logical_tile.h"
#include "backend/common/value.h"

namespace peloton {
namespace executor {

/*
 * A Barrier is used to synchronize a coordinator thread
 * with multiple worker threads.
 * A worker thread calls Release() when its work is done.
 * The coordinator thread calls Wait on a barrier.
 * When the Wait returns, it knows all the worker threads have
 * finished their jobs.
 */
class Barrier {
 public:
  typedef std::uint_least32_t thread_no;
  Barrier(thread_no total): total_(total) { }
  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++count_;
    assert(count_<=total_);
    if(count_==total_)
      cv_.notify_one();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    while(count_<total_)
      cv_.wait(lock);
  }

 private:
  // total number of worker threads
  const thread_no total_;
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t count_ = 0;
};

}
}
