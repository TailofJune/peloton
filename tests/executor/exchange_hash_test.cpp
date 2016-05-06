//
// Created by wendongli on 4/18/16.
//

#include <memory>
#include <chrono>

#include "harness.h"

#include "backend/common/types.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"

#include "backend/executor/hash_join_executor.h"
#include "backend/executor/hash_executor.h"
#include "backend/executor/exchange_hash_executor.h"
#include "backend/executor/merge_join_executor.h"
#include "backend/executor/nested_loop_join_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/expression_util.h"

#include "backend/planner/hash_join_plan.h"
#include "backend/planner/hash_plan.h"
#include "backend/planner/merge_join_plan.h"
#include "backend/planner/nested_loop_join_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tile.h"

#include "backend/concurrency/transaction_manager_factory.h"

#include "mock_executor.h"
#include "executor/executor_tests_util.h"
#include "executor/join_tests_util.h"

using ::testing::NotNull;
using ::testing::Return;
using ::testing::InSequence;

namespace peloton {
namespace test {

class ExchangeHashExecutorTests: public PelotonTest { };

/*
 * create a table with tile_group_num tiles, each of which has row_num rows.
 * heterogeneous table
 */
storage::DataTable *CreateTable(size_t tile_group_num, size_t row_num) {
  std::unique_ptr<storage::DataTable> table(
          ExecutorTestsUtil::CreateTable(row_num, false));
  TestingHarness::GetInstance().GetNextTileGroupId();
  size_t index = 0;
  // the first tile group
  ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(index++), row_num);
  if(tile_group_num==1) {
    return table.release();
  }
  // the second tile group if number of tile groups is even
  if(tile_group_num%2==0) {
    std::vector<catalog::Schema> schemas2(
            {catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0)}),
             catalog::Schema({ExecutorTestsUtil::GetColumnInfo(1),
                              ExecutorTestsUtil::GetColumnInfo(2),
                              ExecutorTestsUtil::GetColumnInfo(3)})});
    std::map<oid_t, std::pair<oid_t, oid_t>> column_map2;
    column_map2[0] = std::make_pair(0, 0);
    column_map2[1] = std::make_pair(1, 0);
    column_map2[2] = std::make_pair(1, 1);
    column_map2[3] = std::make_pair(1, 2);

    table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
            storage::TileGroupFactory::GetTileGroup(
                    INVALID_OID, INVALID_OID,
                    TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
                    schemas2, column_map2, row_num)));
    ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(index++), row_num);
  }
  // all the other tile groups
  tile_group_num = (tile_group_num-1)/2;
  for(size_t i=0; i<tile_group_num; ++i) {
    // Schema for first tile group. Vertical partition is 2, 2.
    std::vector<catalog::Schema> schemas1(
            {catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0),
                              ExecutorTestsUtil::GetColumnInfo(1)}),
             catalog::Schema({ExecutorTestsUtil::GetColumnInfo(2),
                              ExecutorTestsUtil::GetColumnInfo(3)})});

    // Schema for second tile group. Vertical partition is 1, 3.
    std::vector<catalog::Schema> schemas2(
            {catalog::Schema({ExecutorTestsUtil::GetColumnInfo(0)}),
             catalog::Schema({ExecutorTestsUtil::GetColumnInfo(1),
                              ExecutorTestsUtil::GetColumnInfo(2),
                              ExecutorTestsUtil::GetColumnInfo(3)})});

    std::map<oid_t, std::pair<oid_t, oid_t>> column_map1;
    column_map1[0] = std::make_pair(0, 0);
    column_map1[1] = std::make_pair(0, 1);
    column_map1[2] = std::make_pair(1, 0);
    column_map1[3] = std::make_pair(1, 1);

    std::map<oid_t, std::pair<oid_t, oid_t>> column_map2;
    column_map2[0] = std::make_pair(0, 0);
    column_map2[1] = std::make_pair(1, 0);
    column_map2[2] = std::make_pair(1, 1);
    column_map2[3] = std::make_pair(1, 2);

    // Create tile groups.
    table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
            storage::TileGroupFactory::GetTileGroup(
                    INVALID_OID, INVALID_OID,
                    TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
                    schemas1, column_map1, row_num)));

    table->AddTileGroup(std::shared_ptr<storage::TileGroup>(
            storage::TileGroupFactory::GetTileGroup(
                    INVALID_OID, INVALID_OID,
                    TestingHarness::GetInstance().GetNextTileGroupId(), table.get(),
                    schemas2, column_map2, row_num)));

    ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(index++), row_num);
    ExecutorTestsUtil::PopulateTiles(table->GetTileGroup(index++), row_num);
  }

  return table.release();
}

void ExpectNormalTileResults(
        size_t table_tile_group_count, MockExecutor *table_scan_executor,
        std::vector<std::unique_ptr<executor::LogicalTile>> &
        table_logical_tile_ptrs) {
  // Return true for the first table_tile_group_count times
  // Then return false after that
  {
    testing::Sequence execute_sequence;
    for(size_t table_tile_group_itr = 0;
        table_tile_group_itr<table_tile_group_count+1;
        table_tile_group_itr++) {
      // Return true for the first table_tile_group_count times
      if(table_tile_group_itr<table_tile_group_count) {
        EXPECT_CALL(*table_scan_executor, DExecute())
                .InSequence(execute_sequence)
                .WillOnce(Return(true));
      }
      else  // Return false after that
      {
        EXPECT_CALL(*table_scan_executor, DExecute())
                .InSequence(execute_sequence)
                .WillOnce(Return(false));
      }
    }
  }
  // Return the appropriate logical tiles for the first table_tile_group_count
  // times
  {
    testing::Sequence get_output_sequence;
    for(size_t table_tile_group_itr = 0;
        table_tile_group_itr<table_tile_group_count;
        table_tile_group_itr++) {
      EXPECT_CALL(*table_scan_executor, GetOutput())
              .InSequence(get_output_sequence)
              .WillOnce(
                      Return(table_logical_tile_ptrs[table_tile_group_itr].release()));
    }
  }
}

TEST_F(ExchangeHashExecutorTests, CorrectnessTest) {
  constexpr size_t tile_num = 300;
  constexpr size_t row_num = 1000;

  constexpr size_t right_table_tile_group_count = tile_num;

  // Create table.
  std::unique_ptr<storage::DataTable> right_table(CreateTable(tile_num, row_num));
  std::unique_ptr<storage::DataTable> right_table2(CreateTable(tile_num, row_num));

  LOG_INFO("CreateTable done");

  MockExecutor right_table_scan_executor;

  // create tile groups
  std::vector<std::unique_ptr<executor::LogicalTile>>
          right_table_logical_tile_ptrs;
  for(size_t right_table_tile_group_itr = 0;
      right_table_tile_group_itr<right_table_tile_group_count;
      right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
            executor::LogicalTileFactory::WrapTileGroup(
                    right_table->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs.push_back(
            std::move(right_table_logical_tile));
  }

  // Right scan executor returns logical tiles from the right table

  EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

  ExpectNormalTileResults(
          right_table_tile_group_count,
          &right_table_scan_executor,
          right_table_logical_tile_ptrs);

  // Create hash keys
  expression::AbstractExpression *right_table_attr_1 =
          new expression::TupleValueExpression(1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys;
  hash_keys.emplace_back(right_table_attr_1);

  // Create hash plan node
  planner::HashPlan hash_plan_node(hash_keys);

  // Construct the hash executor
  executor::HashExecutor hash_executor(&hash_plan_node, nullptr);
  hash_executor.AddChild(&right_table_scan_executor);

  EXPECT_TRUE(hash_executor.Init());
  EXPECT_TRUE(hash_executor.Execute());

  // Create a same copy for the other hash executor
  MockExecutor right_table_scan_executor2;

  std::vector<std::unique_ptr<executor::LogicalTile>>
          right_table_logical_tile_ptrs2;

  for(size_t right_table_tile_group_itr = 0;
      right_table_tile_group_itr<right_table_tile_group_count;
      right_table_tile_group_itr++) {
    std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
            executor::LogicalTileFactory::WrapTileGroup(
                    right_table2->GetTileGroup(right_table_tile_group_itr)));
    right_table_logical_tile_ptrs2.push_back(
            std::move(right_table_logical_tile));
  }

  // Right scan executor returns logical tiles from the right table

  EXPECT_CALL(right_table_scan_executor2, DInit()).WillOnce(Return(true));

  ExpectNormalTileResults(
          right_table_tile_group_count,
          &right_table_scan_executor2,
          right_table_logical_tile_ptrs2);

  // Create hash keys
  expression::AbstractExpression *right_table_attr_12 =
          new expression::TupleValueExpression(1, 1);

  std::vector<std::unique_ptr<const expression::AbstractExpression>>
          hash_keys2;
  hash_keys2.emplace_back(right_table_attr_12);

  // Create hash plan node
  planner::HashPlan hash_plan_node2(hash_keys2);

  // Construct the hash executor
  executor::ExchangeHashExecutor parallel_hash_executor(&hash_plan_node2, nullptr);
  parallel_hash_executor.AddChild(&right_table_scan_executor2);

  EXPECT_TRUE(parallel_hash_executor.Init());
  EXPECT_TRUE(parallel_hash_executor.Execute());

  // Compare the result hash table of the two hash executors
  // Loop through table 1, ensure that every entry in table 1 is in table 2.
  // Delete the corresponding entry in table 2 for every entry seen in table 1
  // Ensure in the end we have an empty table 2
  auto &hash_table = hash_executor.GetHashTable();
  auto &hash_table2 = parallel_hash_executor.GetHashTable();
  LOG_INFO("hash table size=%lu, concurrent hash table size=%lu",
          (unsigned long)hash_table.size(),
          (unsigned long)hash_table2.size());
  {
    for(const auto &iter1: hash_table) {
      EXPECT_TRUE(hash_table2.contains(iter1.first));
      executor::ExchangeHashExecutor::MapValueType set;
      bool found = hash_table2.find(iter1.first, set);
      EXPECT_TRUE(found);
      for(const auto &iter12: iter1.second) {
        auto iter22 = set.find(iter12);
        EXPECT_TRUE(iter22!=set.end());
        set.erase(iter22);
      }
      EXPECT_TRUE(set.empty());
      bool erased = hash_table2.erase(iter1.first);
      EXPECT_TRUE(erased);
    }
    EXPECT_TRUE(hash_table2.empty());
  }
}


TEST_F(ExchangeHashExecutorTests, SpeedTest) {
  constexpr size_t tile_num = 30000;
  constexpr size_t row_num = 1000;

  // Create table.
  std::unique_ptr<storage::DataTable> right_table(CreateTable(tile_num, row_num));

  LOG_INFO("CreateTable done");

  // Sequential version
  {
    MockExecutor right_table_scan_executor;

    std::vector<std::unique_ptr<executor::LogicalTile>>
            right_table_logical_tile_ptrs;
    constexpr size_t right_table_tile_group_count = tile_num;

    for(size_t right_table_tile_group_itr = 0;
        right_table_tile_group_itr<right_table_tile_group_count;
        right_table_tile_group_itr++) {
      std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
              executor::LogicalTileFactory::WrapTileGroup(
                      right_table->GetTileGroup(right_table_tile_group_itr)));
      right_table_logical_tile_ptrs.push_back(
              std::move(right_table_logical_tile));
    }

    // Right scan executor returns logical tiles from the right table

    EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

    ExpectNormalTileResults(
            right_table_tile_group_count,
            &right_table_scan_executor,
            right_table_logical_tile_ptrs);

    // Create hash keys
    expression::AbstractExpression *right_table_attr_1 =
            new expression::TupleValueExpression(1, 1);

    std::vector<std::unique_ptr<const expression::AbstractExpression>>
            hash_keys;
    hash_keys.emplace_back(right_table_attr_1);

    // Create hash plan node
    planner::HashPlan hash_plan_node(hash_keys);

    // Construct the hash executor
    executor::HashExecutor hash_executor(&hash_plan_node, nullptr);
    hash_executor.AddChild(&right_table_scan_executor);

    const auto start = std::chrono::system_clock::now();
    EXPECT_TRUE(hash_executor.Init());
    EXPECT_TRUE(hash_executor.Execute());
    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> diff = end-start;
    const double ms = diff.count()*1000;
    LOG_INFO("HashExecutor execution time: %lf ms", ms);
  }

  // Parallel version
  double time = 0;
  for(int i=0; i<10; ++i) {
    LOG_INFO("iteration %d", i+1);
    MockExecutor right_table_scan_executor;

    std::vector<std::unique_ptr<executor::LogicalTile>>
            right_table_logical_tile_ptrs;
    constexpr size_t right_table_tile_group_count = tile_num;

    for(size_t right_table_tile_group_itr = 0;
        right_table_tile_group_itr<right_table_tile_group_count;
        right_table_tile_group_itr++) {
      std::unique_ptr<executor::LogicalTile> right_table_logical_tile(
              executor::LogicalTileFactory::WrapTileGroup(
                      right_table->GetTileGroup(right_table_tile_group_itr)));
      right_table_logical_tile_ptrs.push_back(
              std::move(right_table_logical_tile));
    }

    // Right scan executor returns logical tiles from the right table

    EXPECT_CALL(right_table_scan_executor, DInit()).WillOnce(Return(true));

    ExpectNormalTileResults(
            right_table_tile_group_count,
            &right_table_scan_executor,
            right_table_logical_tile_ptrs);

    // Create hash keys
    expression::AbstractExpression *right_table_attr_1 =
            new expression::TupleValueExpression(1, 1);

    std::vector<std::unique_ptr<const expression::AbstractExpression>>
            hash_keys;
    hash_keys.emplace_back(right_table_attr_1);

    // Create hash plan node
    planner::HashPlan hash_plan_node(hash_keys);

    // Construct the hash executor
    executor::ExchangeHashExecutor hash_executor(&hash_plan_node, nullptr);
    hash_executor.AddChild(&right_table_scan_executor);

    const auto start = std::chrono::system_clock::now();
    EXPECT_TRUE(hash_executor.Init());
    EXPECT_TRUE(hash_executor.Execute());
    const auto end = std::chrono::system_clock::now();
    const std::chrono::duration<double> diff = end-start;
    const double ms = diff.count()*1000;
    time += ms;
    LOG_INFO("ExchangeHashExecutor execution time: %lf ms", ms);
  }
  LOG_INFO("ExchangeHashExecutor average time: %lf ms", time/10);
}

}
}

