#include "hash_join.h"
#include <iostream>

void build_orders_hash(const ParquetTable& orders,
                       const std::unordered_set<int64_t>& valid_customers,
                       std::unordered_map<int64_t, OrderInfo>& orders_ht) {

    auto& orderkey_col    = orders.int64_cols[0]; // o_orderkey
    auto& custkey_col     = orders.int64_cols[1]; // o_custkey
    auto& orderdate_col   = orders.int32_cols[0]; // o_orderdate
    auto& shipprio_col    = orders.int32_cols[1]; // o_shippriority

    const int32_t ORDERDATE_CUTOFF = 9221; // 1995-03-15 in int days

    int inserted_count = 0;

    for (size_t i = 0; i < orderkey_col.size(); ++i) {
        int64_t o_custkey = custkey_col[i];
        int64_t o_orderkey = orderkey_col[i];
        int32_t o_orderdate = orderdate_col[i];

        if (valid_customers.count(o_custkey) && o_orderdate < ORDERDATE_CUTOFF) {
            orders_ht[o_orderkey] = {o_orderdate, shipprio_col[i]};
            inserted_count++;

            // Debug first 5 orders
            if (inserted_count <= 5) {
                std::cout << "[DEBUG] Insert orderkey=" << o_orderkey 
                          << " custkey=" << o_custkey
                          << " orderdate=" << o_orderdate
                          << " shipprio=" << shipprio_col[i] << std::endl;
            }
        } else {
            // Log skipped orders
            if (i < 5) {
                std::cout << "[SKIP] orderkey=" << o_orderkey 
                          << " custkey=" << o_custkey
                          << " orderdate=" << o_orderdate
                          << std::endl;
            }
        }
    }

    std::cout << "[INFO] Inserted " << inserted_count << " orders into hash table" << std::endl;
}

void filter_customers(const ParquetTable& customer,
                      std::unordered_set<int64_t>& valid_customers) {
    auto& custkey_col = customer.int64_cols[0]; // c_custkey
    auto& mktseg_col  = customer.string_cols[0]; // c_mktsegment

    const std::string TARGET_SEGMENT = "BUILDING";

    for (size_t i = 0; i < custkey_col.size(); ++i) {
        if (mktseg_col[i] == TARGET_SEGMENT) {
            valid_customers.insert(custkey_col[i]);
        }
    }

    std::cout << "[LOG] filter_customers: " << valid_customers.size() << " valid customers selected" << std::endl;
}