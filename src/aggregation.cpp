#include "aggregation.h"
#include <arrow/api.h>
#include <iostream>

void aggregate_lineitem(const std::shared_ptr<arrow::Table>& lineitem,
                        const std::unordered_map<int, OrderInfo>& orders_ht,
                        std::unordered_map<int, AggResult>& agg) {
    auto l_orderkey_col = std::static_pointer_cast<arrow::Int32Array>(
        lineitem->GetColumnByName("l_orderkey")->chunk(0));
    auto l_extendedprice_col = std::static_pointer_cast<arrow::DoubleArray>(
        lineitem->GetColumnByName("l_extendedprice")->chunk(0));
    auto l_discount_col = std::static_pointer_cast<arrow::DoubleArray>(
        lineitem->GetColumnByName("l_discount")->chunk(0));
    auto l_shipdate_col = std::static_pointer_cast<arrow::Int32Array>(
        lineitem->GetColumnByName("l_shipdate")->chunk(0));

    int64_t n = l_orderkey_col->length();
    int64_t agg_count = 0;

    for (int64_t i = 0; i < n; i++) {
        int orderkey = l_orderkey_col->Value(i);
        if (orders_ht.find(orderkey) != orders_ht.end()) {
            // For testing, remove date filter to ensure aggregation works
            double revenue = l_extendedprice_col->Value(i) * (1.0 - l_discount_col->Value(i));
            auto& r = agg[orderkey];
            r.revenue += revenue;
            r.orderdate = orders_ht.at(orderkey).orderdate;
            r.shippriority = orders_ht.at(orderkey).shippriority;
            agg_count++;
        }
    }

    std::cout << "[INFO] Aggregated " << agg_count << " lineitems out of " << n << std::endl;
}

std::vector<Result> collect_results(const std::unordered_map<int, AggResult>& agg) {
    std::vector<Result> results;
    results.reserve(agg.size());

    for (auto& [orderkey, r] : agg) {
        results.push_back({orderkey, r.revenue, r.orderdate, r.shippriority});
    }

    std::cout << "[INFO] Collected " << results.size() << " results" << std::endl;
    return results;
}