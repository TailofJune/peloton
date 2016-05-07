/*
 * Executor plan for exchange sequential scan
 * Currently not in use
 */

#pragma once

#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_plan.h"
#include "backend/planner/abstract_scan_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/storage/database.h"

namespace peloton {
namespace planner {

class ExchangeSeqScanPlan : public AbstractScan {
 public:
  ExchangeSeqScanPlan(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan &operator=(const ExchangeSeqScanPlan &) = delete;
  ExchangeSeqScanPlan(ExchangeSeqScanPlan &&) = delete;
  ExchangeSeqScanPlan &operator=(ExchangeSeqScanPlan &&) = delete;

  ExchangeSeqScanPlan(const SeqScanPlan *seq_scan_plan)
      : AbstractScan(seq_scan_plan->GetTable(),
                     seq_scan_plan->GetPredicate() == nullptr
                         ? nullptr
                         : seq_scan_plan->GetPredicate()->Copy(),
                     seq_scan_plan->GetColumnIds()) {}

  ExchangeSeqScanPlan(const ExchangeSeqScanPlan *seq_scan_plan)
      : AbstractScan(seq_scan_plan->GetTable(),
                     seq_scan_plan->GetPredicate() == nullptr
                         ? nullptr
                         : seq_scan_plan->GetPredicate()->Copy(),
                     seq_scan_plan->GetColumnIds()) {}

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_EXCHANGE_SEQSCAN;
  }

  const std::string GetInfo() const { return "ExchangeSeqScan"; }

  std::unique_ptr<peloton::planner::AbstractPlan> Copy() const {
    return std::unique_ptr<peloton::planner::AbstractPlan>(
        new ExchangeSeqScanPlan(this));
  }
};

}  // namespace planner
}  // namespace peloton