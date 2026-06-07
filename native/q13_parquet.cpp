#include "q13_parquet.h"

#include "q13_utils.h"

std::unique_ptr<parquet::arrow::FileReader> BuildParquetReader(const fs::path& parquet_path) {
  parquet::ArrowReaderProperties properties;
  properties.set_use_threads(false);

  parquet::arrow::FileReaderBuilder builder;
  CheckStatus(builder.OpenFile(parquet_path.string(), true), "Open parquet file");
  builder.properties(properties);

  return ValueOrThrow(builder.Build(), "Build parquet reader");
}

std::shared_ptr<arrow::Table> ReadParquetColumns(const fs::path& parquet_path,
                                                 const std::vector<int>& column_indices) {
  auto reader = BuildParquetReader(parquet_path);
  std::shared_ptr<arrow::Table> table;
  CheckStatus(reader->ReadTable(column_indices, &table), "Read parquet columns");
  return table;
}

CustomerLayout LoadCustomerLayout(const fs::path& customer_path) {
  auto table = ReadParquetColumns(customer_path, {0});
  auto column = table->column(0);
  CustomerLayout layout;
  layout.num_customers = static_cast<std::size_t>(table->num_rows());

  for (int chunk_index = 0; chunk_index < column->num_chunks(); ++chunk_index) {
    auto base = column->chunk(chunk_index);
    if (base->type_id() != arrow::Type::INT64) {
      Fail("customer c_custkey must be INT64");
    }

    auto array = std::static_pointer_cast<arrow::Int64Array>(base);
    for (int64_t row = 0; row < array->length(); ++row) {
      const auto custkey = static_cast<std::size_t>(array->Value(row));
      layout.max_custkey = std::max(layout.max_custkey, custkey);
    }
  }

  layout.contiguous = layout.max_custkey == layout.num_customers;
  if (!layout.contiguous) {
    layout.valid.assign(layout.max_custkey + 1, 0);
    for (int chunk_index = 0; chunk_index < column->num_chunks(); ++chunk_index) {
      auto array = std::static_pointer_cast<arrow::Int64Array>(column->chunk(chunk_index));
      for (int64_t row = 0; row < array->length(); ++row) {
        layout.valid[static_cast<std::size_t>(array->Value(row))] = 1;
      }
    }
  }

  return layout;
}
