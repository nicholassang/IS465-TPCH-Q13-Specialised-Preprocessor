#pragma once
#include <cstdint>
#include <string>

struct OrderInfo {
    int orderdate;
    int shippriority;
};

struct AggResult {
    double revenue = 0.0;
    int orderdate = 0;
    int shippriority = 0;
};

struct Result {
    int64_t orderkey;
    double revenue;
    std::string orderdate;
    int shippriority;
};