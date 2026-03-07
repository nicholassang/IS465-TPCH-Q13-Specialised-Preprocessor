#include "hash_join.h"
#include <arrow/api.h>
#include <iostream>

void filter_customers(const std::shared_ptr<arrow::Table>& customer,
                      std::unordered_set<int64_t>& valid_customers) {
    auto custkey_col = std::static_pointer_cast<arrow::Int32Array>(
        customer->GetColumnByName("c_custkey")->chunk(0));
    auto mktseg_col = std::static_pointer_cast<arrow::StringArray>(
        customer->GetColumnByName("c_mktsegment")->chunk(0));

    int64_t n = custkey_col->length();
    for (int64_t i = 0; i < n; i++) {
        if (mktseg_col->GetString(i) == "BUILDING") {
            valid_customers.insert(custkey_col->Value(i));
        }
    }
    std::cout << "[INFO] Selected " << valid_customers.size()
              << " valid customers out of " << n << std::endl;
}

void build_orders_hash(const std::shared_ptr<arrow::Table>& orders,
                       const std::unordered_set<int64_t>& valid_customers,
                       std::unordered_map<int64_t, OrderInfo>& orders_ht) {
    auto orderkey_col = std::static_pointer_cast<arrow::Int32Array>(
        orders->GetColumnByName("o_orderkey")->chunk(0));
    auto custkey_col = std::static_pointer_cast<arrow::Int32Array>(
        orders->GetColumnByName("o_custkey")->chunk(0));
    auto orderdate_col = std::static_pointer_cast<arrow::Int32Array>(
        orders->GetColumnByName("o_orderdate")->chunk(0));
    auto shippriority_col = std::static_pointer_cast<arrow::Int32Array>(
        orders->GetColumnByName("o_shippriority")->chunk(0));

    int64_t n = orderkey_col->length();
    int64_t inserted = 0;
    for (int64_t i = 0; i < n; i++) {
        int custkey = custkey_col->Value(i);
        if (valid_customers.find(custkey) != valid_customers.end()) {
            orders_ht[orderkey_col->Value(i)] = {orderdate_col->Value(i), shippriority_col->Value(i)};
            inserted++;
        }
    }
    std::cout << "[INFO] Inserted " << inserted
              << " orders into hash table out of " << n << " total orders" << std::endl;
}