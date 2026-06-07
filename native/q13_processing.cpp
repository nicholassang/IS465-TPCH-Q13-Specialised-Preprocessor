#include "q13_processing.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <cmath>
#include <numeric>

#include "q13_parquet.h"
#include "q13_utils.h"

template <typename StringArrayType>
std::vector<std::uint32_t> ProcessChunk(const arrow::Int64Array& custkeys,
                                        const StringArrayType& comments,
                                        std::size_t counts_size,
                                        const std::vector<std::uint8_t>* valid,
                                        int64_t start_row,
                                        int64_t end_row) {
  std::vector<std::uint32_t> local_counts(counts_size, 0);
  for (int64_t row = start_row; row < end_row; ++row) {
    if (custkeys.IsNull(row) || comments.IsNull(row)) {
      continue;
    }

    const auto custkey = static_cast<std::size_t>(custkeys.Value(row));
    if (custkey >= counts_size) {
      continue;
    }
    if (valid != nullptr && (custkey >= valid->size() || !(*valid)[custkey])) {
      continue;
    }

    const std::string_view comment = comments.GetView(row);
    if (!MatchesSpecialRequests(comment)) {
      ++local_counts[custkey];
    }
  }
  return local_counts;
}

std::vector<std::uint32_t> ProcessOrders(const fs::path& orders_path,
                                         std::size_t counts_size,
                                         const std::vector<std::uint8_t>* valid_customer,
                                         bool log_timings,
                                         int log_every_batches) {
  std::vector<std::uint32_t> counts(counts_size, 0);
  auto reader = BuildParquetReader(orders_path);
  reader->set_batch_size(262144);
  std::vector<int> row_groups(reader->num_row_groups());
  std::iota(row_groups.begin(), row_groups.end(), 0);
  auto batch_reader = ValueOrThrow(
      reader->GetRecordBatchReader(row_groups, {1, 8}), "Create orders record batch reader");

  ScopedTimer progress_timer;
  int processed_batches = 0;
  int64_t processed_rows = 0;
  log_every_batches = std::max(1, log_every_batches);

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    CheckStatus(batch_reader->ReadNext(&batch), "Read next orders batch");
    if (batch == nullptr) {
      break;
    }

    auto key_base = batch->column(0);
    auto comment_base = batch->column(1);
    if (key_base->type_id() != arrow::Type::INT64) {
      Fail("orders o_custkey must be INT64");
    }

    auto key_array = std::static_pointer_cast<arrow::Int64Array>(key_base);
    std::vector<std::uint32_t> local_counts;
    switch (comment_base->type_id()) {
      case arrow::Type::STRING:
        local_counts = ProcessChunk(
            *key_array,
            *std::static_pointer_cast<arrow::StringArray>(comment_base),
            counts_size,
            valid_customer,
            0,
            key_array->length());
        break;
      case arrow::Type::LARGE_STRING:
        local_counts = ProcessChunk(
            *key_array,
            *std::static_pointer_cast<arrow::LargeStringArray>(comment_base),
            counts_size,
            valid_customer,
            0,
            key_array->length());
        break;
      default:
        Fail("orders o_comment must be STRING or LARGE_STRING");
    }

    for (std::size_t i = 0; i < counts.size(); ++i) {
      counts[i] += local_counts[i];
    }

    ++processed_batches;
    processed_rows += batch->num_rows();
    if (log_timings && processed_batches % log_every_batches == 0) {
      std::cout << "[timing] orders progress: batches=" << processed_batches
                << ", rows=" << processed_rows
                << ", elapsed=" << std::fixed << std::setprecision(6)
                << progress_timer.ElapsedSeconds() << "s\n";
    }
  }

  if (log_timings && processed_batches % log_every_batches != 0) {
    std::cout << "[timing] orders progress: batches=" << processed_batches
              << ", rows=" << processed_rows
              << ", elapsed=" << std::fixed << std::setprecision(6)
              << progress_timer.ElapsedSeconds() << "s\n";
  }

  return counts;
}

std::vector<ResultRow> BuildResults(const std::vector<std::uint32_t>& counts,
                                    const CustomerLayout& customer_layout) {
  std::uint32_t max_count = 0;
  if (customer_layout.contiguous) {
    for (std::size_t custkey = 1; custkey <= customer_layout.num_customers; ++custkey) {
      max_count = std::max(max_count, counts[custkey]);
    }
  } else {
    for (std::size_t custkey = 0; custkey < customer_layout.valid.size(); ++custkey) {
      if (customer_layout.valid[custkey]) {
        max_count = std::max(max_count, counts[custkey]);
      }
    }
  }

  std::vector<std::uint32_t> histogram(max_count + 1, 0);
  if (customer_layout.contiguous) {
    for (std::size_t custkey = 1; custkey <= customer_layout.num_customers; ++custkey) {
      ++histogram[counts[custkey]];
    }
  } else {
    for (std::size_t custkey = 0; custkey < customer_layout.valid.size(); ++custkey) {
      if (customer_layout.valid[custkey]) {
        ++histogram[counts[custkey]];
      }
    }
  }

  std::vector<ResultRow> rows;
  rows.reserve(histogram.size());
  for (std::size_t c_count = 0; c_count < histogram.size(); ++c_count) {
    if (histogram[c_count] != 0) {
      rows.push_back(ResultRow{static_cast<int>(c_count), histogram[c_count]});
    }
  }

  std::sort(rows.begin(), rows.end(), [](const ResultRow& left, const ResultRow& right) {
    if (left.custdist != right.custdist) {
      return left.custdist > right.custdist;
    }
    return left.c_count > right.c_count;
  });
  return rows;
}

void WriteResultsCsv(const std::vector<ResultRow>& rows, const fs::path& out_path) {
  std::ofstream out(out_path, std::ios::binary);
  if (!out) {
    Fail("Unable to open output file: " + out_path.string());
  }
  out << "c_count,custdist\n";
  for (const auto& row : rows) {
    out << row.c_count << ',' << row.custdist << '\n';
  }
}

bool CompareCsvOutputs(const fs::path& lhs, const fs::path& rhs) {
  std::ifstream left(lhs, std::ios::binary);
  std::ifstream right(rhs, std::ios::binary);
  if (!left || !right) {
    Fail("Unable to open CSV files for comparison");
  }

  std::string line_left;
  std::string line_right;
  while (true) {
    const bool left_ok = static_cast<bool>(std::getline(left, line_left));
    const bool right_ok = static_cast<bool>(std::getline(right, line_right));
    if (left_ok != right_ok) {
      return false;
    }
    if (!left_ok) {
      return true;
    }
    if (Trim(line_left) != Trim(line_right)) {
      return false;
    }
  }
}
