//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      iter_(table_info_->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == table_info_->table_->End()) {
    return false;
  }

  *tuple = *iter_;
  *rid = tuple->GetRid();

  std::vector<Value> values;
  for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
    values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->Evaluate(tuple, plan_->OutputSchema()));
  }
  *tuple = Tuple(values, plan_->OutputSchema());
  ++iter_;

  auto predict = plan_->GetPredicate();
  if (predict != nullptr && !predict->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) {
    return Next(tuple, rid);
  }

  return false;
}

}  // namespace bustub
