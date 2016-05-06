#pragma once

#include <queue>
#include <atomic>
#include <cassert>
#include <mutex>
#include <condition_variable>

#include "backend/planner/abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {
namespace executor {
class ExchangeSeqScanExecutor: public AbstractExecutor {
 public:
  ExchangeSeqScanExecutor(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor &operator=(const ExchangeSeqScanExecutor &) = delete;
  ExchangeSeqScanExecutor(ExchangeSeqScanExecutor &&) = delete;
  ExchangeSeqScanExecutor &operator=(ExchangeSeqScanExecutor &&) = delete;

  explicit ExchangeSeqScanExecutor(
          const planner::AbstractPlan *node,
          ExecutorContext *executor_context):
          AbstractExecutor(node, executor_context) { }

 protected:
  bool DInit();
  bool DExecute();
  void ScanOneTileGroup(
          const oid_t no,
          concurrency::Transaction *transaction);
 protected:
  /** @brief Selection predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  // whether we have finished scan
  bool done_ = false;
  // lock to protect result_ and finished_number_
  std::mutex result_lock_;
  std::condition_variable cv_;
  std::queue<LogicalTile *> result_;
  oid_t tile_group_number_ = 0;
  oid_t finished_number_ = 0;
};
}
}
