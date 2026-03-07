#pragma once
#include "types.h"
#include <arrow/api.h>
#include <unordered_map>
#include <unordered_set>
#include <memory>

void filter_customers(const std::shared_ptr<arrow::Table>& customer,
                      std::unordered_set<int>& valid_customers);

void build_orders_hash(const std::shared_ptr<arrow::Table>& orders,
                       const std::unordered_set<int>& valid_customers,
                       std::unordered_map<int, OrderInfo>& orders_ht);