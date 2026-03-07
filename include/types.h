#pragma once
#include <cstdint>

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
    int orderkey;
    double revenue;
    int orderdate;
    int shippriority;
};