#include "topk.h"
#include <queue>
#include <algorithm>

std::vector<Result> topk(const std::vector<Result>& results, size_t k) {
    auto cmp = [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue; // descending
        return a.orderdate < b.orderdate;                        // ascending
    };

    std::priority_queue<Result, std::vector<Result>, decltype(cmp)> pq(cmp);

    for (const auto& r : results) {
        pq.push(r);
        if (pq.size() > k) pq.pop();
    }

    std::vector<Result> out;
    out.reserve(pq.size());
    while (!pq.empty()) {
        out.push_back(pq.top());
        pq.pop();
    }
    std::reverse(out.begin(), out.end()); // largest first
    return out;
}