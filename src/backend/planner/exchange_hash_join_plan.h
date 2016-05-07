/*
 * Executor plan for exchange hash join
 * Currently not in use
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"
#include "backend/planner/abstract_join_plan.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace planner {

class ExchangeHashJoinPlan : public AbstractJoinPlan {
 public:
  ExchangeHashJoinPlan(const ExchangeHashJoinPlan &) = delete;
  ExchangeHashJoinPlan &operator=(const ExchangeHashJoinPlan &) = delete;
  ExchangeHashJoinPlan(ExchangeHashJoinPlan &&) = delete;
  ExchangeHashJoinPlan &operator=(ExchangeHashJoinPlan &&) = delete;

  ExchangeHashJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema) {}

  ExchangeHashJoinPlan(
      PelotonJoinType join_type,
      std::unique_ptr<const expression::AbstractExpression> &&predicate,
      std::unique_ptr<const ProjectInfo> &&proj_info,
      std::shared_ptr<const catalog::Schema> &proj_schema,
      const std::vector<oid_t> &outer_hashkeys)
      : AbstractJoinPlan(join_type, std::move(predicate), std::move(proj_info),
                         proj_schema) {
    outer_column_ids_ = outer_hashkeys;  // added for IN-subquery
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_EXCHANGE_HASH_JOIN;
  }

  const std::string GetInfo() const { return "ExchangeHashJoin"; }

  const std::vector<oid_t> &GetOuterHashIds() const {
    return outer_column_ids_;
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    std::unique_ptr<const expression::AbstractExpression> predicate_copy(
        GetPredicate()->Copy());
    std::shared_ptr<const catalog::Schema> schema_copy(
        catalog::Schema::CopySchema(GetSchema()));
    ExchangeHashJoinPlan *new_plan = new ExchangeHashJoinPlan(
        GetJoinType(), std::move(predicate_copy),
        std::move(GetProjInfo()->Copy()), schema_copy, outer_column_ids_);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }

 private:
  std::vector<oid_t> outer_column_ids_;
};

}  // namespace planner
}  // namespace peloton