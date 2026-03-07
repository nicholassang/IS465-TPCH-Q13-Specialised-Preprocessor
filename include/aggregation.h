#pragma once
#include "types.h"
#include <arrow/api.h>
#include <unordered_map>
#include <vector>

void aggregate_lineitem(const std::shared_ptr<arrow::Table>& lineitem,
                        const std::unordered_map<int64_t, OrderInfo>& orders_ht,
                        std::unordered_map<int64_t, AggResult>& agg);

std::vector<Result> collect_results(const std::unordered_map<int64_t, AggResult>& agg);