// include/types.h
#pragma once
#include <unordered_map>
#include <vector>

struct OrderInfo {
    int orderdate;
    int shippriority;
};

struct AggResult {
    double revenue;
    int orderdate;
    int shippriority;
};

struct Result {
    int orderkey;
    double revenue;
    int orderdate;
    int shippriority;
};