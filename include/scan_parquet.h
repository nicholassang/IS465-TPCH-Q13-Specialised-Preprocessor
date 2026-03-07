#pragma once
#include <arrow/api.h>
#include <parquet/arrow/reader.h>
#include <string>
#include <vector>

std::shared_ptr<arrow::Table> read_parquet(
    const std::string& filename, 
    const std::vector<int>& column_indices
);