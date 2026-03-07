#include "scan_parquet.h"
#include "hash_join.h"
#include "aggregation.h"
#include "topk.h"
#include <iostream>
#include "types.h"

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " customer.parquet orders.parquet lineitem.parquet\n";
        return 1;
    }

    auto customer = read_parquet(argv[1], {0,1}); // c_custkey, c_mktsegment
    auto orders   = read_parquet(argv[2], {0,1,2,3}); // o_orderkey,o_custkey,o_orderdate,o_shippriority
    auto lineitem = read_parquet(argv[3], {0,1,2,3}); // l_orderkey,l_extendedprice,l_discount,l_shipdate

    std::unordered_set<int64_t> valid_customers;
    filter_customers(customer, valid_customers);

    std::unordered_map<int64_t, OrderInfo> orders_ht;
    build_orders_hash(orders, valid_customers, orders_ht);

    std::unordered_map<int64_t, AggResult> agg;
    aggregate_lineitem(lineitem, orders_ht, agg);

    auto results = collect_results(agg);
    auto top10 = topk(results, 10);

    for (auto& r : top10) {
        std::cout << r.orderkey << "," << r.revenue << "," << r.orderdate << "," << r.shippriority << "\n";
    }
}