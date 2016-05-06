#include <atomic>
#include <algorithm>
#include <functional>
#include <thread>
#include <condition_variable>
#include <chrono>
#include <utility>

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/exchange_seq_scan_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_header.h"
#include "backend/storage/tile.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/common/logger.h"
#include "backend/common/thread_manager.h"
#include "backend/planner/seq_scan_plan.h"

namespace peloton {
namespace executor {
bool ExchangeSeqScanExecutor::DInit() {
  assert(children_.size()==0||children_.size()==1);
  assert(executor_context_);

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();
  predicate_ = node.GetPredicate();
  column_ids_ = std::move(node.GetColumnIds());

  target_table_ = node.GetTable();
  if(target_table_!=nullptr) {
    tile_group_number_ = target_table_->GetTileGroupCount();
    if(column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/*
 * This method is not supposed to be called by multiple threads
 */
bool ExchangeSeqScanExecutor::DExecute() {
  if(!done_) {
    // Scanning over a logical tile.
    if(children_.size()==1) {
      LOG_TRACE("Exchange Sequential Scan Executor : 1 child ");

      assert(target_table_==nullptr);
      assert(column_ids_.size()==0);
      // In this case, just use sequential scan executor
      while(children_[0]->Execute()) {
        LogicalTile *tile(children_[0]->GetOutput());
        if(predicate_!=nullptr) {
          // Invalidate tuples that don't satisfy the predicate.
          for(oid_t tuple_id : *tile) {
            expression::ContainerTuple<LogicalTile> tuple(tile, tuple_id);
            if(predicate_->Evaluate(&tuple, nullptr, executor_context_)
                    .IsFalse()) {
              tile->RemoveVisibility(tuple_id);
            }
          }
        }
        if(0==tile->GetTupleCount()) {  // Avoid returning empty tiles
          continue;
        }
        result_.push(tile);
      }
    }
      // Scanning a table
    else if(children_.size()==0) {
      LOG_TRACE("Exchange Sequential Scan Executor : 0 child ");

      assert(target_table_!=nullptr);
      assert(target_table_->GetTileGroupCount()==tile_group_number_);
      assert(column_ids_.size()>0);

      // In this case, wrap the scanning of each tile group into an individual task
      // Break the whole execution into multiple tasks
      for(oid_t no=0; no<tile_group_number_; ++no)
        ThreadManager::GetInstance().AddTask(
                std::bind(&ExchangeSeqScanExecutor::ScanOneTileGroup, this,
                          no, concurrency::current_txn));
      {
        std::unique_lock<std::mutex> lock(result_lock_);
        while(finished_number_<tile_group_number_)
          cv_.wait(lock);
      }
    }
    done_ = true;
  }
  // Having finished the entire scan
  // Return results
  assert(done_);
  assert(finished_number_==tile_group_number_);
  if(result_.empty())
    return false;
  SetOutput(result_.front());
  result_.pop();
  return true;
}

void ExchangeSeqScanExecutor::ScanOneTileGroup(const oid_t no, concurrency::Transaction *transaction) {
  concurrency::current_txn = transaction;
  auto tile_group = target_table_->GetTileGroup(no);
  oid_t active_tuple_count = tile_group->GetNextTupleSlot();

  // Construct position list by looping through tile group
  // and applying the predicate.
  std::vector<oid_t> position_list;

  for(oid_t tuple_id = 0; tuple_id<active_tuple_count; tuple_id++) {
    if(predicate_==nullptr) {
      position_list.push_back(tuple_id);
    }
    else {
      expression::ContainerTuple<storage::TileGroup> tuple(
              tile_group.get(), tuple_id);
      auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_)
              .IsTrue();
      if(eval==true) {
        position_list.push_back(tuple_id);
      }
    }
  }

  {
    std::lock_guard<std::mutex> guard(result_lock_);
    // Don't return empty tiles
    if(!position_list.empty()) {
      // Construct logical tile.
      LogicalTile *logical_tile = LogicalTileFactory::GetTile();
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(position_list));
      result_.push(logical_tile);
    }

    ++finished_number_;
    if(finished_number_==tile_group_number_)
      cv_.notify_one();
  }
}
}
}