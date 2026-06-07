#ifndef Q13_PARQUET_H
#define Q13_PARQUET_H

#include <filesystem>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "parquet/arrow/reader.h"

namespace fs = std::filesystem;

#include "q13_types.h"

std::unique_ptr<parquet::arrow::FileReader> BuildParquetReader(const fs::path& parquet_path);

std::shared_ptr<arrow::Table> ReadParquetColumns(const fs::path& parquet_path,
                                                 const std::vector<int>& column_indices);

CustomerLayout LoadCustomerLayout(const fs::path& customer_path);

#endif  // Q13_PARQUET_H
