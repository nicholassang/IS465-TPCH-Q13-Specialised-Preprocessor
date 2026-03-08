#pragma once
#include "types.h"
#include "scan_parquet.h"
#include <unordered_map>
#include <vector>
#include <iostream>

void print_lineitem_schema(const ParquetTable& lineitem) {
    std::cout << "print_lineitem_schema: [DEBUG] Lineitem Table Schema:" << std::endl;

    // Hardcoded column names from your Parquet schema
    std::vector<std::string> int64_names = {"l_orderkey", "l_extendedprice", "l_discount"};
    std::vector<std::string> int32_names = {"l_shipdate"};

    // INT64 columns
    for (size_t i = 0; i < lineitem.int64_cols.size(); i++) {
        std::string name = (i < int64_names.size()) ? int64_names[i] : "INT64_col_" + std::to_string(i);
        std::cout << "print_lineitem_schema: [DEBUG] INT64 column: " << name
                  << ", rows=" << lineitem.int64_cols[i].size() << std::endl;
    }

    // DOUBLE columns (if any)
    for (size_t i = 0; i < lineitem.double_cols.size(); i++) {
        std::string name = "DOUBLE_col_" + std::to_string(i);
        std::cout << "print_lineitem_schema: [DEBUG] DOUBLE column: " << name
                  << ", rows=" << lineitem.double_cols[i].size() << std::endl;
    }

    // INT32 columns
    for (size_t i = 0; i < lineitem.int32_cols.size(); i++) {
        std::string name = (i < int32_names.size()) ? int32_names[i] : "INT32_col_" + std::to_string(i);
        std::cout << "print_lineitem_schema: [DEBUG] INT32 column: " << name
                  << ", rows=" << lineitem.int32_cols[i].size() << std::endl;
    }

    std::cout << "===============================" << std::endl;
}

void aggregate_lineitem(const ParquetTable& lineitem,
                        const std::unordered_map<int64_t, OrderInfo>& orders_ht,
                        std::unordered_map<int64_t, AggResult>& agg) {
    // Debug: Print lineitem schema and validate required columns
    print_lineitem_schema(lineitem);
    // Validate that required columns exist
    if (lineitem.int64_cols.size() < 3 ) {
        std::cerr << "aggregate_lineitem: [ERROR] orderkey_col extended_col discount_colmissing required columns!" << std::endl;
        return;
    }
    if (lineitem.int32_cols.size() < 1) {
        std::cerr << "aggregate_lineitem: [ERROR] shipdate_col missing required columns!" << std::endl;
        return;
    }

    auto& orderkey_col  = lineitem.int64_cols[0];  // l_orderkey
    auto& extended_col  = lineitem.int64_cols[1]; // l_extendedprice
    auto& discount_col  = lineitem.int64_cols[2]; // l_discount
    auto& shipdate_col  = lineitem.int32_cols[0];  // l_shipdate

    size_t n = orderkey_col.size();
    if (extended_col.size() != n || discount_col.size() != n || shipdate_col.size() != n) {
        std::cerr << "aggregate_lineitem: [ERROR] Column size mismatch in lineitem table!" << std::endl;
        return;
    }

    std::cout << "aggregate_lineitem: [DEBUG] Starting aggregation for " << n << " lineitems" << std::endl;

    int64_t aggregated_count = 0;
    int64_t skipped_count = 0;

    for (size_t i = 0; i < n; i++) {
        int64_t orderkey = orderkey_col[i];
        int32_t shipdate = shipdate_col[i];

        auto it = orders_ht.find(orderkey);
        if (it == orders_ht.end()) {
            skipped_count++;
            continue;
        }

        double revenue = extended_col[i] * (1.0 - discount_col[i]);
        AggResult& r = agg[orderkey];
        r.revenue += revenue;
        r.orderdate = it->second.orderdate;
        r.shippriority = it->second.shippriority;

        aggregated_count++;

        if (aggregated_count % 100000 == 0) {
            std::cout << "aggregate_lineitem: [DEBUG] Aggregated " << aggregated_count
                      << " lineitems, skipped " << skipped_count << std::endl;
            std::cout.flush();
        }
    }

    std::cout << "aggregate_lineitem: [INFO] Aggregation complete: " << aggregated_count 
              << " aggregated, " << skipped_count << " skipped." << std::endl;
    std::cout.flush();
    std::cout << "==============================="<< std::endl;
}

std::vector<Result> collect_results(const std::unordered_map<int64_t, AggResult>& agg) {
    std::vector<Result> results;
    results.reserve(agg.size());

    for (auto& [orderkey, r] : agg) {
        results.push_back({orderkey, r.revenue, r.orderdate, r.shippriority});
    }

    std::cout << "collect_results: [INFO] Collected " << results.size() << " results" << std::endl;
    return results;
}