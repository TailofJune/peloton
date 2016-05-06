//
// Created by Lu Zhang on 4/10/16.
//

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/exchange_hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
    ExchangeHashJoinExecutor::ExchangeHashJoinExecutor(const planner::AbstractPlan *node,
                                                       ExecutorContext *executor_context)
      : AbstractJoinExecutor(node, executor_context){
    }



    bool ExchangeHashJoinExecutor::DInit() {
      assert(children_.size() == 2);

      auto status = AbstractJoinExecutor::DInit();
      if (status == false) return status;


      const planner::AbstractJoinPlan &node____ =
        GetPlanNode<planner::AbstractJoinPlan>();

      join_type_ = node____.GetJoinType();
      if (join_type_ == JOIN_TYPE_INNER) {
        printf("init : join_type: inner\n");
      }else if (join_type_ == JOIN_TYPE_LEFT){

        printf("init : join_type: left\n");
      }else if (join_type_ == JOIN_TYPE_RIGHT){

        printf("init : join_type: right\n");
      }else if (join_type_ == JOIN_TYPE_OUTER){

        printf("init : join_type: outer\n");
      }else if (join_type_ == JOIN_TYPE_INVALID){

        printf("init : join_type: invalid \n");
      }else {

        printf("init : join_type: unknown\n");
      }

      //todo: child node type needs modification.
//      assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);
//      LOG_INFO("children_[0].type %d", children_[0]->GetRawNode()->GetPlanNodeType() );
      hash_executor_ = reinterpret_cast<ExchangeHashExecutor *>(children_[1]);
      //hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);

      atomic_left_matching_idx = 0;
      atomic_right_matching_idx = 0;
      printf("Exchange Hash Join Executor init\n");
      return true;
    }


    void ExchangeHashJoinExecutor::GetRightHashTable(ExBarrier * barrier){

      const auto start = std::chrono::system_clock::now();

      printf("Build Right Child Hash Table Task picked up \n");
      while (children_[1]->Execute()) {
//        printf("get one right child\n");
        BufferRightTile(children_[1]->GetOutput());
      }
      printf("Building Right Child Hashtable Phase done. \n");
      printf("hash_table size: %lu\n", hash_executor_->GetHashTable().size());
      printf("right result tiles size: %lu\n", right_result_tiles_.size());
      barrier->Release();


      const auto end = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end-start;
      const double ms = diff.count()*1000;
      printf("Get Right Hash Table takes %lf ms\n", ms);

    }

    void ExchangeHashJoinExecutor::GetLeftScanResult(ExBarrier * barrier){

      const auto start = std::chrono::system_clock::now();
      printf("Build Left Child Scan Task picked up \n");
      while(children_[0]->Execute()){
        BufferLeftTile(children_[0]->GetOutput());
      }
      printf("Get Left Child Done. \n");
      if (left_result_tiles_.size() == 0){
        printf("left child size: %lu\n", left_result_tiles_.size());
      }else {
        printf("left_result_tiles.size():%lu, tuple num per tile:%lu\n", left_result_tiles_.size(), left_result_tiles_.back().get()->GetTupleCount());
      }
      barrier->Release();
      const auto end = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end-start;
      const double ms = diff.count()*1000;
      printf("Get Left Scan takes %lf ms\n", ms);
    }


    void ExchangeHashJoinExecutor::Probe(std::atomic<thread_no> *no,
                                         PesudoBarrier *barrier) {

      printf("Probe Task picked up \n");
      const auto start = std::chrono::system_clock::now();

      const thread_no self_no = (*no)++;
      const size_t begin_idx = self_no * SIZE_PER_PARTITION;
      const size_t end_idx = std::min(begin_idx + SIZE_PER_PARTITION,  left_result_tiles_.size());

      // todo: in this case, this algorithm is still parallel. Because, only the number of
      // calling this function.

//      LOG_INFO("Probe Task picked up \n");
//      printf("Probe Task picked up \n");
//      printf("GetTupleCount: %lu\n", left_result_tiles_.back()->GetTupleCount());

      auto &hash_table = hash_executor_->GetHashTable();
      auto &hashed_col_ids = hash_executor_->GetHashKeyIds();



      // iterate over its task range tiles
      for (size_t cur_idx = begin_idx; cur_idx < end_idx; cur_idx++) {
        LogicalTile *left_tile = left_result_tiles_[cur_idx].get();

        oid_t prev_tile = INVALID_OID;
        std::unique_ptr<LogicalTile> output_tile;
        LogicalTile::PositionListsBuilder pos_lists_builder;


        // Go over the left tile
        for (auto left_tile_itr : *left_tile) {

          const expression::ContainerTuple<executor::LogicalTile> left_tuple(
            left_tile, left_tile_itr, &hashed_col_ids);

          executor::ExchangeHashExecutor::MapValueType right_tuples;

//          std::unordered_set<std::pair<size_t, oid_t>,
//            boost::hash<std::pair<size_t, oid_t>>> right_tuples;
          bool if_match = hash_table.find(left_tuple, right_tuples);
//          bool if_match = hash_table.contains(left_tuple);
          //auto got = hash_table.find(left_tuple);
          if (if_match) {
          //if (got != hash_table.end()) {
//            auto right_tuples = hash_table.find(left_tuple);
            //auto right_tuples = got->second;
            RecordMatchedLeftRow(cur_idx, left_tile_itr);



            // Go over the matching right tuples
            const expression::ContainerTuple<executor::LogicalTile> left_tuple_test(
                        left_tile, left_tile_itr);

            for (auto &location : right_tuples) {

              /*-------For testing cuckoo hashing -----------------------------*/

              /*
              LogicalTile *right_tmp_tile = right_result_tiles_[location.first].get();
              const expression::ContainerTuple<executor::LogicalTile> right_tuple(
                right_tmp_tile, location.second);

              int left_tuple_join_attribute_val = left_tuple_test.GetValue(1).GetIntegerForTestsOnly();
              int right_tuple_join_attribute_val = right_tuple.GetValue(1).GetIntegerForTestsOnly();
              printf("testing 22222 --------- left value: %d, right value: %d\n", left_tuple_join_attribute_val, right_tuple_join_attribute_val);
              */
              /*-------For testing cuckoo hashing -----------------------------*/

              // Check if we got a new right tile itr
              if (prev_tile != location.first) {
                // Check if we have any join tuples from last prev_tile
                if (pos_lists_builder.Size() > 0) {
                  output_tile->SetPositionListsAndVisibility(
                    pos_lists_builder.Release());
                  lockfree_buffered_output_tiles.push(output_tile.release());
                }

                // Get the logical tile from right child
                LogicalTile *right_tile = right_result_tiles_[location.first].get();

                // Build output logical tile
                output_tile = BuildOutputLogicalTile(left_tile, right_tile);

                // Build position lists
                pos_lists_builder =
                  LogicalTile::PositionListsBuilder(left_tile, right_tile);

                pos_lists_builder.SetRightSource(
                  &right_result_tiles_[location.first]->GetPositionLists());
              }


              // Add join tuple
              pos_lists_builder.AddRow(left_tile_itr, location.second);

              RecordMatchedRightRow(location.first, location.second);

              // Cache prev logical tile itr
              prev_tile = location.first;
            }
          }
        }
        // Check if we have any join tuples
        if (pos_lists_builder.Size() > 0) {
          LOG_TRACE("Join tile size : %lu \n", pos_lists_builder.Size());
          output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
          lockfree_buffered_output_tiles.push(output_tile.release());
        }
      }


      const auto end = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end-start;
      const double ms = diff.count()*1000;
      printf("Probe thread %u takes %lf ms\n", (unsigned)self_no, ms);

      LOG_TRACE("Probe() thread %u done", (unsigned)self_no);
      barrier->Release();

    }



/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
    bool ExchangeHashJoinExecutor::DExecute() {

      // Loop until we have non-empty result tile or exit
      for (; ;) {
        // Check if we have any buffered output tiles
        if (lockfree_buffered_output_tiles.empty() == false) {
          LogicalTile *output_tile = nullptr;
//          bool ret = lockfree_buffered_output_tiles.pop(output_tile);
//          assert(ret);
          lockfree_buffered_output_tiles.pop(output_tile);
          SetOutput(output_tile);
          // exit 0
          return true;
        }


        // option1. build right hashTable and collect left child at the same time
        // option2. build right hashTable and then collect part of left child every time

        // Here takes option1.
        if (prepare_children_ == false) {
          // build right hashTable
          ExBarrier build_hashtable_barrier(1);
          std::function<void()> build_hashtable_worker =
            std::bind(&ExchangeHashJoinExecutor::GetRightHashTable, this, &build_hashtable_barrier);
          LaunchWorkerThreads(1, build_hashtable_worker);
          printf("Wait for right child build to finish.\n");

          // collect all left children
          ExBarrier collect_scan_result_barrier(1);
          std::function<void()> collect_scan_result_worker =
            std::bind(&ExchangeHashJoinExecutor::GetLeftScanResult, this, &collect_scan_result_barrier);
          LaunchWorkerThreads(1, collect_scan_result_worker);
          printf("Wait for left child scan to finish.\n");

          build_hashtable_barrier.Wait();
          collect_scan_result_barrier.Wait();
          printf("Ready to Probe.\n");

          // todo: How can I do that:
          // Once I find right_child is empty, stop collect left children.
          if ((right_result_tiles_.size() == 0)
              && (join_type_ == JOIN_TYPE_INNER || join_type_ == JOIN_TYPE_RIGHT)) {
            no_need_to_probe_ = true;

            // exit 1
            return false;
          } else if ((left_result_tiles_.size() == 0)
                     && (join_type_ == JOIN_TYPE_INNER || join_type_ == JOIN_TYPE_LEFT)) {
            no_need_to_probe_ = true;
            // exit2
            return false;
          }


          main_start = std::chrono::system_clock::now();

          // partition sub tasks
          size_t left_child_size = left_result_tiles_.size();
          if (left_child_size == 0){
            probe_barrier_.SetNeedToDo(true);
          }
          size_t partition_number = left_child_size / SIZE_PER_PARTITION;
          if (left_child_size % SIZE_PER_PARTITION != 0) {
            ++partition_number;
          }
          printf("left_result_tiles.size():%lu, partition num in probe:%lu\n", left_child_size, partition_number);


          // sub tasks begin
          std::atomic<thread_no> no(0);
//          Barrier probe_barrier(partition_number);
          probe_barrier_.SetTotal(partition_number);
          std::function<void()> probe_worker =
            std::bind(&ExchangeHashJoinExecutor::Probe, this,
                      &no, &probe_barrier_);
//        LaunchWorkerThreads(partition_number - 1, probe_worker);
          LaunchWorkerThreads(partition_number, probe_worker);


          // here main thread also pariticipate in doing sub tasks.
          // todo: consider alternatives: main thread push result on by on
//        Probe(&no, &probe_barrier);

//          probe_barrier.Wait();
          prepare_children_ = true;
        }


        // return ret (ont by one)
        if (lockfree_buffered_output_tiles.empty() == false) {
          LogicalTile *output_tile = nullptr;
//          bool ret = lockfree_buffered_output_tiles.pop(output_tile);
//          assert(ret);
          lockfree_buffered_output_tiles.pop(output_tile);
          SetOutput(output_tile);
          return true;
        } else {
//          if (no_need_to_probe_ == false &&
          if (probe_barrier_.IsNoNeedToDo() == false &&
              probe_barrier_.IsDone() == false){
            continue;
          }
          main_end = std::chrono::system_clock::now();
          const std::chrono::duration<double> diff = main_end - main_start;
          const double ms = diff.count()*1000;
          printf("Inner part takes %lf ms \n", ms);

          if (BuildOuterJoinOutput()){
            continue;
          } else{
            LOG_INFO("real finish\n");
            return false;
          }

        }

      }
    }



    //todo: 1. parallel real outer join (not empty right child)
    bool ExchangeHashJoinExecutor::BuildLeftJoinOutput() {
      printf("ExchangeHashJoinExecutor::BuildLeftJoinOutput called.\n");
      auto curt_left_matching_idx = atomic_left_matching_idx.fetch_add(0);
//      printf("curt_left_matching_idx : %lu\n", curt_left_matching_idx);
      while (curt_left_matching_idx  < no_matching_left_row_sets_.size()) {
        if (no_matching_left_row_sets_[curt_left_matching_idx].empty()) {
//          printf("curt_left_matching_idx : %lu is empty\n", curt_left_matching_idx);
          curt_left_matching_idx = atomic_left_matching_idx.fetch_add(1) + 1;
//          printf("curt_left_matching_idx : %lu\n", curt_left_matching_idx);
          continue;
        }
//        printf("availble curt_left_matching_idx : %lu\n", curt_left_matching_idx);

        std::unique_ptr<LogicalTile> output_tile(nullptr);
        auto left_tile = left_result_tiles_[curt_left_matching_idx ].get();
        LogicalTile::PositionListsBuilder pos_lists_builder;
        if (right_result_tiles_.size() == 0) {
          // no tile information for right tile. construct a output tile from left
          // tile only
          output_tile = BuildOutputLogicalTile(left_tile, nullptr, proj_schema_);
          pos_lists_builder = LogicalTile::PositionListsBuilder(
            &(left_tile->GetPositionLists()), nullptr);
        } else {
          assert(right_result_tiles_.size() > 0);
          // construct the output tile from both children tiles
          auto right_tile = right_result_tiles_.front().get();
          output_tile = BuildOutputLogicalTile(left_tile, right_tile);
          pos_lists_builder =
            LogicalTile::PositionListsBuilder(left_tile, right_tile);
        }
        // add rows with null values on the right
        for (auto left_row_itr : no_matching_left_row_sets_[curt_left_matching_idx ]) {
          pos_lists_builder.AddRightNullRow(left_row_itr);
        }

        assert(pos_lists_builder.Size() > 0);

        output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
        lockfree_buffered_output_tiles.push(output_tile.release());

        atomic_left_matching_idx.fetch_add(1);
        return true;
      }
      LOG_INFO("ExchangeHashJoinExecutor::BuildLeftJoinOutput return false.");
      return false;
    }

/*
 * build right join output by adding null rows for every row from left tile
 * which doesn't have a match
 */
    //todo: parallel real outer join (not empty right child)
    bool ExchangeHashJoinExecutor::BuildRightJoinOutput() {
//      printf("ExchangeHashJoinExecutor::BuildRightJoinOutput called.\n");
      auto curt_right_matching_idx = atomic_right_matching_idx.fetch_add(0);
//      printf("curt_right_matching_idx :%lu, exhj_no_matching_right_row_sets_: %lu\n", curt_right_matching_idx, exhj_no_matching_right_row_sets_.size());
      while (curt_right_matching_idx  < exhj_no_matching_right_row_sets_.size()) {
        if (exhj_no_matching_right_row_sets_[curt_right_matching_idx ].Empty()) {
          curt_right_matching_idx = atomic_right_matching_idx.fetch_add(1);
          continue;
        }

        std::unique_ptr<LogicalTile> output_tile(nullptr);
        auto right_tile = right_result_tiles_[curt_right_matching_idx ].get();
        LogicalTile::PositionListsBuilder pos_lists_builder;
        if (left_result_tiles_.size() == 0) {
          // no tile information for left tile. construct a output tile from right
          // tile only
          output_tile = BuildOutputLogicalTile(nullptr, right_tile, proj_schema_);
          pos_lists_builder = LogicalTile::PositionListsBuilder(
            nullptr, &(right_tile->GetPositionLists()));
        } else {
          assert(left_result_tiles_.size() > 0);
          // construct the output tile from both children tiles
          auto left_tile = left_result_tiles_.front().get();
          output_tile = BuildOutputLogicalTile(left_tile, right_tile);
          pos_lists_builder =
            LogicalTile::PositionListsBuilder(left_tile, right_tile);
        }
        // add rows with null values on the left
        for (auto right_row_itr : exhj_no_matching_right_row_sets_[curt_right_matching_idx ].container_) {
          pos_lists_builder.AddLeftNullRow(right_row_itr);
        }
        assert(pos_lists_builder.Size() > 0);


        output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
        lockfree_buffered_output_tiles.push(output_tile.release());


        atomic_right_matching_idx.fetch_add(1);
        return true;
      }
      printf("ExchangeHashJoinExecutor::BuildRightJoinOutput return false.\n");
      return false;
    }

/**
  * Update the row set with all rows from the last tile from left child
  */
    void ExchangeHashJoinExecutor::UpdateLeftJoinRowSets() {
      assert(left_result_tiles_.size() - no_matching_left_row_sets_.size() == 1);
      no_matching_left_row_sets_.emplace_back(left_result_tiles_.back()->begin(),
                                              left_result_tiles_.back()->end());

//      ConcurrentOidSet set;
//      set.container_ = std::unordered_set<oid_t>(left_result_tiles_.back()->begin(),
//                     left_result_tiles_.back()->end());
//      exhj_no_matching_left_row_sets_.emplace_back(std::move(set));
    }

/**
  * Update the row set with all rows from the last tile from right child
  */
    void ExchangeHashJoinExecutor::UpdateRightJoinRowSets() {
      assert(right_result_tiles_.size() - exhj_no_matching_right_row_sets_.size() == 1);
      ConcurrentOidSet set;
      // not sure if move is safe....
//      set.container_ = std::move(std::unordered_set<oid_t>(right_result_tiles_.back()->begin(),
//                                                           right_result_tiles_.back()->end()));
      set.container_ = std::unordered_set<oid_t>(right_result_tiles_.back()->begin(),
                                                           right_result_tiles_.back()->end());

      exhj_no_matching_right_row_sets_.emplace_back(std::move(set));


//      assert(right_result_tiles_.size() - no_matching_right_row_sets_.size() == 1);
//      exhj_no_matching_right_row_sets_.emplace_back(right_result_tiles_.back()->begin(),
//                                               right_result_tiles_.back()->end());
    }



  }  // namespace executor
}  // namespace peloton
