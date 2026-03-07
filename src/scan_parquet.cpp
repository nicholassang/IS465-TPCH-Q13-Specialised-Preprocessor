#include "scan_parquet.h"
#include <iostream>
#include <stdexcept>
#include <cmath>

ParquetTable read_parquet(const std::string& filename, const std::vector<int>& column_indices) {
    std::cout << "[INFO] Opening Parquet file: " << filename << std::endl;

    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename, false);

    auto file_metadata = reader->metadata();
    int num_columns = file_metadata->num_columns();
    int64_t num_rows = file_metadata->num_rows();

    ParquetTable table;
    table.num_rows = num_rows;
    table.num_cols = num_columns;

    std::vector<int> cols = column_indices;
    if (cols.empty()) {
        for (int c = 0; c < num_columns; c++) cols.push_back(c);
    }

    for (int col_index : cols) {
        auto column_reader = reader->RowGroup(0)->Column(col_index);
        parquet::Type::type t = file_metadata->schema()->Column(col_index)->physical_type();

        if (t == parquet::Type::INT32) {
            auto int_reader = static_cast<parquet::Int32Reader*>(column_reader.get());
            std::vector<int32_t> data(num_rows);
            int64_t values_read = 0;
            int16_t def_level;
            int32_t value;
            while (int_reader->HasNext()) {
                int_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                data[values_read - 1] = value;
            }
            table.int32_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::INT64) {
            auto int_reader = static_cast<parquet::Int64Reader*>(column_reader.get());
            std::vector<int64_t> data(num_rows);
            int64_t values_read = 0;
            int16_t def_level;
            int64_t value;
            while (int_reader->HasNext()) {
                int_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                data[values_read - 1] = value;
            }
            table.int64_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::DOUBLE) {
            auto dbl_reader = static_cast<parquet::DoubleReader*>(column_reader.get());
            std::vector<double> data(num_rows);
            int64_t values_read = 0;
            int16_t def_level;
            double value;
            while (dbl_reader->HasNext()) {
                dbl_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                data[values_read - 1] = value;
            }
            table.double_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::BYTE_ARRAY) {
            auto ba_reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());
            std::vector<std::string> data(num_rows);
            int64_t values_read = 0;
            int16_t def_level;
            parquet::ByteArray value;
            while (ba_reader->HasNext()) {
                ba_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                data[values_read - 1] = std::string(reinterpret_cast<const char*>(value.ptr), value.len);
            }
            table.string_cols.push_back(std::move(data));
        }
        else {
            throw std::runtime_error("Unsupported column type");
        }
    }

    std::cout << "[INFO] Successfully read table with "
              << table.num_rows << " rows and "
              << table.num_cols << " columns." << std::endl;

    return table;
}