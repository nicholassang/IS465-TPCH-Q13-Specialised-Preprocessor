#include "hash_join.h"
#include <iostream>

void filter_customers(const ParquetTable& customer, std::unordered_set<int64_t>& valid_customers) {
    auto& custkey_col = customer.int64_cols[0];
    auto& mktseg_col = customer.string_cols[0];
    int64_t n = custkey_col.size();

    for (int64_t i = 0; i < n; i++) {
        if (mktseg_col[i] == "BUILDING") {
            valid_customers.insert(custkey_col[i]);
        }
    }
    std::cout << "[INFO] Selected " << valid_customers.size() << " valid customers out of " << n << std::endl;
}

void build_orders_hash(const ParquetTable& orders, const std::unordered_set<int64_t>& valid_customers,
                       std::unordered_map<int64_t, OrderInfo>& orders_ht) {
    auto& orderkey_col = orders.int64_cols[0];
    auto& custkey_col = orders.int64_cols[1];
    auto& orderdate_col = orders.int32_cols[0];
    auto& shippriority_col = orders.int32_cols[1];

    int64_t n = orderkey_col.size();
    int64_t inserted = 0;

    for (int64_t i = 0; i < n; i++) {
        if (valid_customers.find(custkey_col[i]) != valid_customers.end()) {
            orders_ht[orderkey_col[i]] = {orderdate_col[i], shippriority_col[i]};
            inserted++;
        }
    }

    std::cout << "[INFO] Inserted " << inserted << " orders into hash table out of " << n << " total orders" << std::endl;
}