#pragma once
#include <parquet/api/reader.h>
#include <string>
#include <vector>
#include <unordered_map>

struct ParquetTable {
    std::vector<std::vector<int64_t>> int64_cols;
    std::vector<std::vector<int32_t>> int32_cols;
    std::vector<std::vector<double>> double_cols;
    std::vector<std::vector<std::string>> string_cols;
    int64_t num_rows = 0;
    int64_t num_cols = 0;
};

ParquetTable read_parquet(const std::string& filename, const std::vector<int>& column_indices = {});