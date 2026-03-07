#include "aggregation.h"
#include <iostream>

void aggregate_lineitem(const ParquetTable& lineitem,
                        const std::unordered_map<int64_t, OrderInfo>& orders_ht,
                        std::unordered_map<int64_t, AggResult>& agg) {
    auto& orderkey_col = lineitem.int64_cols[0];       // l_orderkey
    auto& extended_col = lineitem.double_cols[0];      // l_extendedprice
    auto& discount_col = lineitem.double_cols[1];      // l_discount
    auto& shipdate_col = lineitem.int32_cols[0];       // l_shipdate

    int64_t aggregated_count = 0;

    for (int64_t i = 0; i < orderkey_col.size(); i++) {
        int64_t orderkey = orderkey_col[i];
        if (orders_ht.find(orderkey) != orders_ht.end()) {
            if (shipdate_col[i] > 9204) { // days since 1970
                double revenue = extended_col[i] * (1.0 - discount_col[i]);
                auto& r = agg[orderkey];
                r.revenue += revenue;
                r.orderdate = orders_ht.at(orderkey).orderdate;
                r.shippriority = orders_ht.at(orderkey).shippriority;
                aggregated_count++;
            }
        }
    }

    std::cout << "[INFO] Aggregated " << aggregated_count << " lineitems out of " << orderkey_col.size() << std::endl;
}

std::vector<Result> collect_results(const std::unordered_map<int64_t, AggResult>& agg) {
    std::vector<Result> results;
    results.reserve(agg.size());

    for (auto& [orderkey, r] : agg) {
        results.push_back({orderkey, r.revenue, r.orderdate, r.shippriority});
    }

    std::cout << "[INFO] Collected " << results.size() << " results" << std::endl;
    return results;
}