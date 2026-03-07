#include "scan_parquet.h"
#include <arrow/io/api.h>
#include <arrow/result.h>
#include "types.h"

std::shared_ptr<arrow::Table> read_parquet(
    const std::string& filename, 
    const std::vector<int>& column_indices
) {
    auto infile_result = arrow::io::ReadableFile::Open(filename);
    auto infile = infile_result.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);

    std::shared_ptr<arrow::Table> table;
    if (column_indices.empty()) {
        reader->ReadTable(&table);
    } else {
        reader->ReadTable(column_indices, &table);
    }

    return table;
}