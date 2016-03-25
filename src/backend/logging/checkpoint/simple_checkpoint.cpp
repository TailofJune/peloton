/*-------------------------------------------------------------------------
 *
 * simple_checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint/simple_checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <dirent.h>
#include <sys/stat.h>

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/logging/checkpoint/simple_checkpoint.h"
#include "backend/logging/loggers/wal_frontend_logger.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/logging/records/transaction_record.h"
#include "backend/logging/log_record.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_manager.h"
#include "backend/concurrency/transaction.h"
#include "backend/executor/executors.h"
#include "backend/executor/executor_context.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/catalog/manager.h"
#include "backend/storage/tile.h"
#include "backend/storage/database.h"

#include "backend/common/logger.h"
#include "backend/common/types.h"

// configuration for testing
// extern int64_t peloton_checkpoint_interval;

namespace peloton {
namespace logging {
//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

size_t GetLogFileSize(int log_file_fd);

bool IsFileTruncated(FILE *log_file, size_t size_to_read, size_t log_file_size);

size_t GetNextFrameSize(FILE *log_file, size_t log_file_size);

LogRecordType GetNextLogRecordType(FILE *log_file, size_t log_file_size);

// bool ReadTransactionRecordHeader(TransactionRecord &txn_record, FILE
// *log_file,
//                                 size_t log_file_size);

bool ReadTupleRecordHeader(TupleRecord &tuple_record, FILE *log_file,
                           size_t log_file_size);

storage::Tuple *ReadTupleRecordBody(catalog::Schema *schema, VarlenPool *pool,
                                    FILE *log_file, size_t log_file_size);

// Wrappers
storage::DataTable *GetTable(TupleRecord tupleRecord);

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//
SimpleCheckpoint &SimpleCheckpoint::GetInstance() {
  static SimpleCheckpoint simple_checkpoint;
  return simple_checkpoint;
}

void SimpleCheckpoint::Init() {
  // TODO check configuration
  if (true) {
    std::thread checkpoint_thread(&SimpleCheckpoint::DoCheckpoint, this);
    checkpoint_thread.detach();
  }
}

void SimpleCheckpoint::DoCheckpoint() {
  sleep(checkpoint_interval_);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto &log_manager = LogManager::GetInstance();
  logger_ = log_manager.GetBackendLogger();

  while (true) {
    sleep(checkpoint_interval_);

    // build executor context
    std::unique_ptr<concurrency::Transaction> txn(
        txn_manager.BeginTransaction());
    assert(txn);
    assert(txn.get());
    LOG_TRACE("Txn ID = %lu ", txn->GetTransactionId());

    std::unique_ptr<executor::ExecutorContext> executor_context(
        new executor::ExecutorContext(
            txn.get(), bridge::PlanTransformer::BuildParams(nullptr)));
    LOG_TRACE("Building the executor tree");

    auto &catalog_manager = catalog::Manager::GetInstance();
    auto database_count = catalog_manager.GetDatabaseCount();
    bool failure = false;

    for (oid_t database_idx = 0; database_idx < database_count && !failure;
         database_idx++) {
      auto database = catalog_manager.GetDatabase(database_idx);
      auto table_count = database->GetTableCount();
      auto database_oid = database->GetOid();
      for (oid_t table_idx = 0; table_idx < table_count && !failure;
           table_idx++) {
        /* Get the target table */
        storage::DataTable *target_table = database->GetTable(table_idx);
        assert(target_table);
        LOG_INFO("SeqScan: database oid %lu table oid %lu: %s", database_idx,
                 table_idx, target_table->GetName().c_str());

        auto schema = target_table->GetSchema();
        assert(schema);
        expression::AbstractExpression *predicate = nullptr;
        std::vector<oid_t> column_ids;
        column_ids.resize(schema->GetColumnCount());
        std::iota(column_ids.begin(), column_ids.end(), 0);

        /* Construct the Peloton plan node */
        LOG_TRACE("Initializing the executor tree");
        std::unique_ptr<planner::SeqScanPlan> scan_plan_node(
            new planner::SeqScanPlan(target_table, predicate, column_ids));
        std::unique_ptr<executor::SeqScanExecutor> scan_executor(
            new executor::SeqScanExecutor(scan_plan_node.get(),
                                          executor_context.get()));
        if (!Execute(scan_executor.get(), txn.get(), target_table,
                     database_oid)) {
          break;
        }
      }
    }

    if (records_.size() > 0) {
      CreateCheckpointFile();
      Persist();
    }
  };
}

bool SimpleCheckpoint::DoRecovery() {
  // open log file and file descriptor
  // we open it in read + binary mode
  std::string file_name = "peloton_checkpoint.log";
  checkpoint_file_ = fopen(file_name.c_str(), "rb");

  if (checkpoint_file_ == NULL) {
    LOG_ERROR("Checkpoint File is NULL");
    return false;
  }

  // also, get the descriptor
  checkpoint_file_fd_ = fileno(checkpoint_file_);
  if (checkpoint_file_fd_ == INVALID_FILE_DESCRIPTOR) {
    LOG_ERROR("checkpoint_file_fd_ is -1");
    return false;
  }

  checkpoint_file_size_ = GetLogFileSize(checkpoint_file_fd_);
  assert(checkpoint_file_size_ > 0);
  while (true) {
    auto record_type =
        GetNextLogRecordType(checkpoint_file_, checkpoint_file_size_);
    if (record_type == LOGRECORD_TYPE_WAL_TUPLE_INSERT) {
      InsertTuple();
    } else if (record_type == LOGRECORD_TYPE_TRANSACTION_COMMIT) {
      break;
    }
  }

  // After finishing recovery, set the next oid with maximum oid
  // observed during the recovery
  auto &manager = catalog::Manager::GetInstance();
  manager.SetNextOid(max_oid_);

  return true;
}

void SimpleCheckpoint::InsertTuple() {
  TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  // Check for torn log write
  if (ReadTupleRecordHeader(tuple_record, checkpoint_file_,
                            checkpoint_file_size_) == false) {
    LOG_ERROR("Could not read tuple record header.");
    return;
  }

  auto table = GetTable(tuple_record);

  // Read off the tuple record body from the log
  auto tuple = ReadTupleRecordBody(table->GetSchema(), pool.get(),
                                   checkpoint_file_, checkpoint_file_size_);

  // Check for torn log write
  if (tuple == nullptr) {
    return;
  }

  auto target_location = tuple_record.GetInsertLocation();
  auto tile_group_id = target_location.block;
  auto tuple_slot = target_location.offset;

  auto &manager = catalog::Manager::GetInstance();
  auto tile_group = manager.GetTileGroup(tile_group_id);

  // Create new tile group if table doesn't already have that tile group
  if (tile_group == nullptr) {
    table->AddTileGroupWithOid(tile_group_id);
    tile_group = manager.GetTileGroup(tile_group_id);
    if (max_oid_ < tile_group_id) {
      max_oid_ = tile_group_id;
    }
  }

  // Do the insert!
  auto inserted_tuple_slot =
      tile_group->InsertTupleFromCheckpoint(tuple_slot, tuple);

  if (inserted_tuple_slot == INVALID_OID) {
    // TODO: We need to abort on failure!
  } else {
    // txn->RecordInsert(target_location);
    table->IncreaseNumberOfTuplesBy(1);
  }
  delete tuple;
}

bool SimpleCheckpoint::Execute(executor::AbstractExecutor *scan_executor,
                               concurrency::Transaction *txn,
                               storage::DataTable *target_table,
                               oid_t database_oid) {
  // Prepare columns
  auto schema = target_table->GetSchema();
  std::vector<oid_t> column_ids;
  column_ids.resize(schema->GetColumnCount());
  std::iota(column_ids.begin(), column_ids.end(), 0);

  // Initialize the seq scan executor
  auto status = scan_executor->Init();
  // Abort and cleanup
  if (status == false) {
    return false;
  }
  LOG_TRACE("Running the seq scan executor");

  // Execute seq scan until we get result tiles
  for (;;) {
    status = scan_executor->Execute();
    // Stop
    if (status == false) {
      break;
    }

    // Retrieve a logical tile
    std::unique_ptr<executor::LogicalTile> logical_tile(
        scan_executor->GetOutput());
    auto tile_group_id = logical_tile->GetColumnInfo(0)
                             .base_tile->GetTileGroup()
                             ->GetTileGroupId();

    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          logical_tile.get(), tuple_id);

      // Logging
      {
        // construct a physical tuple from the logical tuple
        std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
        for (auto column_id : column_ids) {
          tuple->SetValue(column_id, cur_tuple.GetValue(column_id),
                          this->pool.get());
        }
        ItemPointer location(tile_group_id, tuple_id);
        assert(logger_);
        auto record = logger_->GetTupleRecord(
            LOGRECORD_TYPE_TUPLE_INSERT, txn->GetTransactionId(),
            target_table->GetOid(), location, INVALID_ITEMPOINTER, tuple.get(),
            database_oid);
        assert(record);
        CopySerializeOutput output_buffer;
        record->Serialize(output_buffer);
        records_.push_back(record);
      }
    }
  }
  LogRecord *record = new TransactionRecord(LOGRECORD_TYPE_TRANSACTION_COMMIT);
  CopySerializeOutput output_buffer;
  record->Serialize(output_buffer);
  records_.push_back(record);
  return true;
}

void SimpleCheckpoint::CreateCheckpointFile() {
  // open checkpoint file and file descriptor
  // we open it in append + binary mode

  // TODO multiple versions of checkpoint
  std::string file_name = "peloton_checkpoint.log";
  checkpoint_file_ = fopen(file_name.c_str(), "ab+");
  if (checkpoint_file_ == NULL) {
    LOG_ERROR("Checkpoint File is NULL");
  }
  assert(checkpoint_file_);
  // also, get the descriptor
  checkpoint_file_fd_ = fileno(checkpoint_file_);
  if (checkpoint_file_fd_ == INVALID_FILE_DESCRIPTOR) {
    LOG_ERROR("log_file_fd is -1");
  }
}

// Only called when checkpoint has actual contents
void SimpleCheckpoint::Persist() {
  assert(checkpoint_file_);
  assert(records_.size() > 0);
  assert(checkpoint_file_fd_ != INVALID_FILE_DESCRIPTOR);
  // First, write all the record in the queue
  for (auto record : records_) {
    assert(record);
    assert(record->GetMessageLength() > 0);
    fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
           checkpoint_file_);
  }

  // Then, flush
  int ret = fflush(checkpoint_file_);
  if (ret != 0) {
    LOG_ERROR("Error occured in fflush(%d)", ret);
  }

  // Finally, sync
  ret = fsync(checkpoint_file_fd_);
  // fsync_count++;
  if (ret != 0) {
    LOG_ERROR("Error occured in fsync(%d)", ret);
  }

  // Clean up the frontend logger's queue
  for (auto record : records_) {
    delete record;
  }
  records_.clear();
}

void SimpleCheckpoint::SetLogger(BackendLogger *logger) { logger_ = logger; }

}  // namespace logging
}  // namespace peloton