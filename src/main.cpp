#include "scan_parquet.h"
#include "hash_join.h"
#include "aggregation.h"
#include "topk.h"
#include <iostream>
#include <fstream>
#include "types.h"

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " customer.parquet orders.parquet lineitem.parquet\n";
        return 1;
    }
    std::cout << "START\n";

    // Read Parquet files
    auto customer = read_parquet(argv[1], {0,1}); // c_custkey, c_mktsegment
    auto orders   = read_parquet(argv[2], {0,1,2,3}); // o_orderkey,o_custkey,o_orderdate,o_shippriority
    auto lineitem = read_parquet(argv[3], {0,1,2,3}); // l_orderkey,l_extendedprice,l_discount,l_shipdate

    std::cout << "[DEBUG] Testing columns " << std::endl;

    std::cout << "[DEBUG] First 10 lineitem orderkeys: ";
    for(int i=0;i<10;i++) std::cout<<lineitem.int64_cols[0][i]<<" ";
    std::cout<<std::endl;

    std::cout << "[DEBUG] First 10 orders orderkeys: ";
    for(int i=0;i<10;i++) std::cout<<orders.int64_cols[0][i]<<" ";
    std::cout<<std::endl;

    // Filter customers
    std::unordered_set<int64_t> valid_customers;
    filter_customers(customer, valid_customers);

    std::cout << "[LOG] Valid customers count: " << valid_customers.size() << "\n";

    // Debug: show first 10 valid customers
    int count = 0;
    std::cout << "[DEBUG] Sample valid customers: ";
    for (auto v : valid_customers) {
        std::cout << v << " ";
        if (++count >= 10) break;
    }
    std::cout << std::endl;

    // Build orders hash table
    std::unordered_map<int64_t, OrderInfo> orders_ht;
    build_orders_hash(orders, valid_customers, orders_ht);
    std::cout << "[LOG] Orders inserted into hash table: " << orders_ht.size()
              << " / " << orders.int64_cols[0].size() << std::endl;

    // Debug: check first few orders
    count = 0;
    std::cout << "[DEBUG] Sample orders hash keys: ";
    for (auto& [k, v] : orders_ht) {
        std::cout << k << " ";
        if (++count >= 10) break;
    }

    // Aggregate lineitems
    std::unordered_map<int64_t, AggResult> agg;
    try {
        aggregate_lineitem(lineitem, orders_ht, agg);
    } catch (const std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
    }

    std::cout << "RESULTS" << std::endl;

    // Collect results
    auto results = collect_results(agg);
    auto top10 = topk(results, 10);

    // Print CSV header
    std::cout << "l_orderkey,revenue,o_orderdate,o_shippriority" << std::endl;

    // Print top 10 rows
    for (auto& r : top10) {
        std::cout << r.orderkey << "," 
                  << r.revenue << "," 
                  << r.orderdate << "," 
                  << r.shippriority << std::endl;
    }

    std::cout << std::flush;
}