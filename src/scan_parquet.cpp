#include "scan_parquet.h"
#include <iostream>

std::shared_ptr<arrow::Table> read_parquet(
    const std::string& filename,
    const std::vector<int>& column_indices
) {
    std::cout << "[INFO] Opening Parquet file: " << filename << std::endl;

    auto infile_res = arrow::io::ReadableFile::Open(filename);
    if (!infile_res.ok()) {
        std::cerr << "[ERROR] Failed to open file: " << filename << std::endl;
        exit(1);
    }
    auto infile = infile_res.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);

    std::shared_ptr<arrow::Table> table;
    if (column_indices.empty()) {
        reader->ReadTable(&table);
    } else {
        reader->ReadTable(column_indices, &table);
    }

    std::cout << "[INFO] Successfully read table with "
              << table->num_rows() << " rows and "
              << table->num_columns() << " columns." << std::endl;

    return table;
}