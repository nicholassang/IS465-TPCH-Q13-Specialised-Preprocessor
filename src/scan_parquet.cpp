#include "scan_parquet.h"
#include <iostream>
#include <stdexcept>
#include <cmath>

ParquetTable read_parquet(const std::string& filename, const std::vector<int>& column_indices) {
    std::cout << "Opening Parquet file: " << filename << std::endl;

    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename, false);

    auto file_metadata = reader->metadata();
    int num_columns = file_metadata->num_columns();
    // Log column info
    std::cout << "Columns in " << filename << ":\n";
    for (int c = 0; c < num_columns; c++) {
        auto col_schema = file_metadata->schema()->Column(c);
        std::string col_name = col_schema->name();
        parquet::Type::type physical_type = col_schema->physical_type();

        std::string type_str;
        switch (physical_type) {
            case parquet::Type::INT32: type_str = "INT32"; break;
            case parquet::Type::INT64: type_str = "INT64"; break;
            case parquet::Type::DOUBLE: type_str = "DOUBLE"; break;
            case parquet::Type::BYTE_ARRAY: type_str = "BYTE_ARRAY"; break;
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: type_str = "FIXED_LEN_BYTE_ARRAY"; break;
            default: type_str = "UNKNOWN"; break;
        }

        std::cout << "Column " << c << ": " << col_name << ", type=" << type_str << std::endl;
    }

    int64_t num_rows = file_metadata->num_rows();

    int num_row_groups = file_metadata->num_row_groups();
    std::cout << "Num row groups: " << num_row_groups << std::endl;

    ParquetTable table;
    table.num_rows = num_rows;
    table.num_cols = num_columns;

    std::vector<int> cols = column_indices;
    if (cols.empty()) {
        for (int c = 0; c < num_columns; c++) cols.push_back(c);
    }

    for (int col_index : cols) {
        parquet::Type::type t = file_metadata->schema()->Column(col_index)->physical_type();
        std::cout << "[INFO] Reading column " << col_index
                  << " type=" << t
                  << ", expecting " << num_rows << " rows" << std::endl;

        if (t == parquet::Type::INT32) {
            std::vector<int32_t> data;
            data.reserve(num_rows);
            for (int rg = 0; rg < num_row_groups; rg++) {
                auto reader_rg = reader->RowGroup(rg)->Column(col_index);
                auto int_reader = static_cast<parquet::Int32Reader*>(reader_rg.get());
                int16_t def_level;
                int32_t value;
                while (int_reader->HasNext()) {
                    int64_t values_read = 0;
                    int_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                    if (values_read > 0) data.push_back(value);
                }
            }
            std::cout << "[INFO] First 5 int32 values: ";
            for (size_t i = 0; i < std::min<size_t>(5, data.size()); i++) std::cout << data[i] << " ";
            std::cout << std::endl;
            table.int32_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::INT64) {
            std::vector<int64_t> data;
            data.reserve(num_rows);
            for (int rg = 0; rg < num_row_groups; rg++) {
                auto reader_rg = reader->RowGroup(rg)->Column(col_index);
                auto int_reader = static_cast<parquet::Int64Reader*>(reader_rg.get());
                int16_t def_level;
                int64_t value;
                while (int_reader->HasNext()) {
                    int64_t values_read = 0;
                    int_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                    if (values_read > 0) data.push_back(value);
                }
            }
            std::cout << "[INFO] First 5 int64 values: ";
            for (size_t i = 0; i < std::min<size_t>(5, data.size()); i++) std::cout << data[i] << " ";
            std::cout << std::endl;
            table.int64_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::DOUBLE) {
            std::vector<double> data;
            data.reserve(num_rows);
            for (int rg = 0; rg < num_row_groups; rg++) {
                auto reader_rg = reader->RowGroup(rg)->Column(col_index);
                auto dbl_reader = static_cast<parquet::DoubleReader*>(reader_rg.get());
                int16_t def_level;
                double value;
                while (dbl_reader->HasNext()) {
                    int64_t values_read = 0;
                    dbl_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                    if (values_read > 0) data.push_back(value);
                }
            }
            std::cout << "[INFO] First 5 double values: ";
            for (size_t i = 0; i < std::min<size_t>(5, data.size()); i++) std::cout << data[i] << " ";
            std::cout << std::endl;
            table.double_cols.push_back(std::move(data));
        }
        else if (t == parquet::Type::BYTE_ARRAY) {
            std::vector<std::string> data;
            data.reserve(num_rows);
            for (int rg = 0; rg < num_row_groups; rg++) {
                auto reader_rg = reader->RowGroup(rg)->Column(col_index);
                auto ba_reader = static_cast<parquet::ByteArrayReader*>(reader_rg.get());
                int16_t def_level;
                parquet::ByteArray value;
                while (ba_reader->HasNext()) {
                    int64_t values_read = 0;
                    ba_reader->ReadBatch(1, &def_level, nullptr, &value, &values_read);
                    if (values_read > 0) data.emplace_back(reinterpret_cast<const char*>(value.ptr), value.len);
                }
            }
            std::cout << "[INFO] First 5 string values: ";
            for (size_t i = 0; i < std::min<size_t>(5, data.size()); i++) std::cout << "\"" << data[i] << "\" ";
            std::cout << std::endl;
            table.string_cols.push_back(std::move(data));
        }
        else {
            throw std::runtime_error("Unsupported column type");
        }
    }

    std::cout << "Successfully read table with "
              << table.num_rows << " rows and "
              << table.num_cols << " columns." << std::endl;

    return table;
}