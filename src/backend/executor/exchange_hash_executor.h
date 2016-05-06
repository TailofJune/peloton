//
// Created by wendongli on 5/5/16.
//

#pragma once

#include <unordered_map>
#include <unordered_set>

#include "backend/common/types.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "backend/executor/logical_tile.h"
#include "backend/expression/container_tuple.h"
#include "abstract_scan_executor.h"

#include <boost/functional/hash.hpp>
#include <backend/common/barrier.h>

namespace peloton {
namespace executor {

class ExchangeHashExecutor : public AbstractExecutor {
 public:
  ExchangeHashExecutor(const ExchangeHashExecutor &) = delete;
  ExchangeHashExecutor &operator=(const ExchangeHashExecutor &) = delete;
  ExchangeHashExecutor(ExchangeHashExecutor &&) = delete;
  ExchangeHashExecutor &operator=(ExchangeHashExecutor &&) = delete;

  explicit ExchangeHashExecutor(const planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

  typedef std::unordered_set<std::pair<size_t, oid_t>,
          boost::hash<std::pair<size_t, oid_t>>> MapValueType;

  typedef cuckoohash_map<
          expression::ContainerTuple<LogicalTile>,
          MapValueType,
          expression::ContainerTupleHasher<LogicalTile>,
          expression::ContainerTupleComparator<LogicalTile>
  > HashMapType;

  inline HashMapType &GetHashTable() { return this->hash_table_; }

  inline const std::vector<oid_t> &GetHashKeyIds() const {
    return this->column_ids_;
  }

  void BuildHashTableThreadMain(LogicalTile *tile, size_t child_tile_itr, Barrier *barrier);

 protected:
  bool DInit();

  bool DExecute();

 private:
  inline void EnsureTableSize() { hash_table_.reserve((size_t)(child_tiles_.size()*0.8)); }
  /** @brief Hash table */
  HashMapType hash_table_;

  /** @brief Input tiles from child node */
  std::vector<LogicalTile *> child_tiles_;

  std::vector<oid_t> column_ids_;

  bool done_ = false;

  size_t result_itr = 0;
};


}  // namespace executor
}   // namespace peloton