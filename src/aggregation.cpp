#include "aggregation.h"
#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/type.h>
#include <unordered_map>
#include <vector>
#include <iostream>
#include "types.h"

void aggregate_lineitem(
    const std::shared_ptr<arrow::Table>& lineitem,
    const std::unordered_map<int64_t, OrderInfo>& orders_ht,
    std::unordered_map<int64_t, AggResult>& agg
) {
    int64_t n_rows = lineitem->num_rows();
    int64_t n_cols = lineitem->num_columns();

    std::cout << "[INFO] Aggregating lineitem table with " << n_rows
              << " rows and " << n_cols << " columns" << std::endl;

    // Log column names, types, and chunk info
    for (int c = 0; c < n_cols; c++) {
        auto column = lineitem->column(c);
        auto field = lineitem->schema()->field(c);
        std::cout << "[INFO] Column " << c << ": " << field->name()
                  << ", type=" << field->type()->ToString()
                  << ", num_chunks=" << column->num_chunks() << std::endl;

        for (int chunk_idx = 0; chunk_idx < column->num_chunks(); chunk_idx++) {
            auto chunk = column->chunk(chunk_idx);
            std::cout << "  Chunk " << chunk_idx << " length=" << chunk->length() << std::endl;
            std::cout << "   First 5 values: ";
            for (int i = 0; i < std::min<int64_t>(5, chunk->length()); i++) {
                if (field->type()->id() == arrow::Type::INT32) {
                    auto arr = std::static_pointer_cast<arrow::Int32Array>(chunk);
                    std::cout << arr->Value(i) << " ";
                } else if (field->type()->id() == arrow::Type::INT64) {
                    auto arr = std::static_pointer_cast<arrow::Int64Array>(chunk);
                    std::cout << arr->Value(i) << " ";
                } else if (field->type()->id() == arrow::Type::DECIMAL128) {
                    auto arr = std::static_pointer_cast<arrow::Decimal128Array>(chunk);
                    std::cout << arr->FormatValue(i) << " ";
                } else if (field->type()->id() == arrow::Type::DATE32) {
                    auto arr = std::static_pointer_cast<arrow::Date32Array>(chunk);
                    std::cout << arr->Value(i) << " ";
                } else {
                    std::cout << "[unknown type] ";
                }
            }
            std::cout << std::endl;
        }
    }

    // Columns we actually use
    auto l_orderkey_col = std::static_pointer_cast<arrow::Int32Array>(
        lineitem->GetColumnByName("l_orderkey")->chunk(0)
    );
    auto l_extendedprice_col = std::static_pointer_cast<arrow::Decimal128Array>(
        lineitem->GetColumnByName("l_extendedprice")->chunk(0)
    );
    auto l_discount_col = std::static_pointer_cast<arrow::Decimal128Array>(
        lineitem->GetColumnByName("l_discount")->chunk(0)
    );
    auto l_shipdate_col = std::static_pointer_cast<arrow::Int32Array>(
        lineitem->GetColumnByName("l_shipdate")->chunk(0)
    );

    // Cast decimal types to access scale
    auto ext_type = std::static_pointer_cast<arrow::Decimal128Type>(
        l_extendedprice_col->type()
    );
    auto disc_type = std::static_pointer_cast<arrow::Decimal128Type>(
        l_discount_col->type()
    );

    int64_t aggregated_count = 0;
    for (int64_t i = 0; i < l_orderkey_col->length(); i++) {
        int64_t orderkey = l_orderkey_col->Value(i);
        if (orders_ht.find(orderkey) != orders_ht.end()) {
            if (l_shipdate_col->Value(i) > 9204) { // days since 1970
                // Convert Decimal128 -> double using scale
                double ext = std::stod(l_extendedprice_col->FormatValue(i));
                double disc = std::stod(l_discount_col->FormatValue(i));

                double revenue = ext * (1.0 - disc);

                auto& r = agg[orderkey];
                r.revenue += revenue;
                r.orderdate = orders_ht.at(orderkey).orderdate;
                r.shippriority = orders_ht.at(orderkey).shippriority;

                aggregated_count++;
            }
        }
    }

    std::cout << "[INFO] Aggregated " << aggregated_count 
              << " lineitems out of " << l_orderkey_col->length() << std::endl;
}

std::vector<Result> collect_results(
    const std::unordered_map<int64_t, AggResult>& agg
) {
    std::vector<Result> results;
    results.reserve(agg.size());

    for (auto& [orderkey, r] : agg) {
        results.push_back({orderkey, r.revenue, r.orderdate, r.shippriority});
    }

    std::cout << "[INFO] Collected " << results.size() << " results" << std::endl;
    return results;
}