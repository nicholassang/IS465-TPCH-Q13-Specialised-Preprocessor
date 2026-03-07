#include "topk.h"
#include <queue>
#include <vector>

std::vector<Result> topk(const std::vector<Result>& results, size_t k) {
    auto cmp = [](const Result& a, const Result& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.orderdate < b.orderdate;
    };
    std::priority_queue<Result, std::vector<Result>, decltype(cmp)> pq(cmp);

    for (auto& r : results) {
        pq.push(r);
        if (pq.size() > k) pq.pop();
    }

    std::vector<Result> out;
    while (!pq.empty()) {
        out.push_back(pq.top());
        pq.pop();
    }
    std::reverse(out.begin(), out.end());
    return out;
}