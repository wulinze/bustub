//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)),
    table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
    indexes_info_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void InsertExecutor::Init() {
    if (!plan_->IsRawInsert()) {
        child_executor_->Init();
    } else {
        cursor = 0;
    }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
    if (!plan_->IsRawInsert()) {
        if (!child_executor_->Next(tuple, rid)) {
            return false;
        }
    } else {
        auto values = plan_->RawValues();
        if (cursor >= static_cast<uint32_t>(values.size())) {
            return false;
        }

        *tuple = Tuple(values[cursor], plan_->OutputSchema());
        *rid = tuple->GetRid();
    }
    if (!table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
        return false;
    }
    for (auto&& index_info : indexes_info_) {
        auto key_tuple = tuple->KeyFromTuple(table_info_->schema_, index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }

    return Next(tuple, rid);
}

}  // namespace bustub
