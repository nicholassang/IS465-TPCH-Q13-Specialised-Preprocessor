#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <cmath>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "parquet/arrow/reader.h"

namespace fs = std::filesystem;

struct ResultRow {
  int c_count;
  std::uint32_t custdist;
};

struct Timings {
  double customer_scan_s = 0.0;
  double orders_scan_and_filter_s = 0.0;
  double histogram_build_s = 0.0;
  double final_sort_s = 0.0;
  double output_write_s = 0.0;
  double processing_total_s = 0.0;
  double total_s = 0.0;
};

struct RunOutput {
  std::vector<ResultRow> results;
  Timings timings;
};

struct Options {
  std::optional<fs::path> data_dir;
  std::optional<std::string> sf;
  std::optional<fs::path> out_path;
  std::optional<fs::path> compare_to;
  int benchmark_runs = 0;
  bool log_timings = false;
  int log_every_batches = 25;
};

class ScopedTimer {
 public:
  ScopedTimer() : start_(Clock::now()) {}

  double ElapsedSeconds() const {
    return std::chrono::duration<double>(Clock::now() - start_).count();
  }

 private:
  using Clock = std::chrono::steady_clock;
  Clock::time_point start_;
};

[[noreturn]] void Fail(const std::string& message) {
  throw std::runtime_error(message);
}

void CheckStatus(const arrow::Status& status, const std::string& context) {
  if (!status.ok()) {
    Fail(context + ": " + status.ToString());
  }
}

template <typename T>
T ValueOrThrow(arrow::Result<T> result, const std::string& context) {
  if (!result.ok()) {
    Fail(context + ": " + result.status().ToString());
  }
  return std::move(result).ValueUnsafe();
}

std::string Trim(const std::string& value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

bool MatchesSpecialRequests(std::string_view comment) {
  const auto first = comment.find("special");
  if (first == std::string_view::npos) {
    return false;
  }
  const auto second = comment.find("requests", first + 7);
  return second != std::string_view::npos;
}

std::unique_ptr<parquet::arrow::FileReader> BuildParquetReader(const fs::path& parquet_path) {
  parquet::ArrowReaderProperties properties;
  properties.set_use_threads(true);

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

struct CustomerLayout {
  std::vector<std::uint8_t> valid;
  std::size_t max_custkey = 0;
  std::size_t num_customers = 0;
  bool contiguous = false;
};

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

template <typename StringArrayType>
std::vector<std::uint32_t> ProcessChunkParallel(const arrow::Int64Array& custkeys,
                                                const StringArrayType& comments,
                                                std::size_t counts_size,
                                                const std::vector<std::uint8_t>* valid) {
  const unsigned int hardware_threads = std::max(1u, std::thread::hardware_concurrency());
  const int64_t rows = custkeys.length();
  const int64_t target_tasks = std::min<int64_t>(hardware_threads, std::max<int64_t>(1, rows / 65536));

  if (target_tasks <= 1) {
    return ProcessChunk(custkeys, comments, counts_size, valid, 0, rows);
  }

  std::vector<std::future<std::vector<std::uint32_t>>> tasks;
  tasks.reserve(static_cast<std::size_t>(target_tasks));
  for (int64_t task_index = 0; task_index < target_tasks; ++task_index) {
    const int64_t start_row = (rows * task_index) / target_tasks;
    const int64_t end_row = (rows * (task_index + 1)) / target_tasks;
    tasks.push_back(std::async(std::launch::async,
                               [&custkeys, &comments, counts_size, valid, start_row, end_row]() {
      return ProcessChunk(custkeys, comments, counts_size, valid, start_row, end_row);
    }));
  }

  std::vector<std::uint32_t> merged(counts_size, 0);
  for (auto& task : tasks) {
    auto partial = task.get();
    for (std::size_t i = 0; i < merged.size(); ++i) {
      merged[i] += partial[i];
    }
  }
  return merged;
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
        local_counts = ProcessChunkParallel(
            *key_array,
            *std::static_pointer_cast<arrow::StringArray>(comment_base),
            counts_size,
            valid_customer);
        break;
      case arrow::Type::LARGE_STRING:
        local_counts = ProcessChunkParallel(
            *key_array,
            *std::static_pointer_cast<arrow::LargeStringArray>(comment_base),
            counts_size,
            valid_customer);
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

RunOutput RunQ13(const fs::path& data_dir,
                 const std::optional<fs::path>& out_path,
                 bool log_timings,
                 int log_every_batches) {
  const auto customer_path = data_dir / "customer.parquet";
  const auto orders_path = data_dir / "orders.parquet";
  ScopedTimer total_timer;

  if (log_timings) {
    std::cout << "[timing] starting q13 run on " << data_dir.string() << "\n";
  }

  ScopedTimer stage_timer;
  auto customer_layout = LoadCustomerLayout(customer_path);
  const double customer_s = stage_timer.ElapsedSeconds();
  if (log_timings) {
    std::cout << "[timing] customer stage complete: elapsed=" << std::fixed
              << std::setprecision(6) << customer_s
              << "s, cumulative=" << customer_s << "s\n";
  }

  stage_timer = ScopedTimer();
  auto counts = ProcessOrders(
      orders_path,
      customer_layout.max_custkey + 1,
      customer_layout.contiguous ? nullptr : &customer_layout.valid,
      log_timings,
      log_every_batches);
  const double orders_s = stage_timer.ElapsedSeconds();
  if (log_timings) {
    std::cout << "[timing] orders stage complete: elapsed=" << std::fixed
              << std::setprecision(6) << orders_s
              << "s, cumulative=" << (customer_s + orders_s) << "s\n";
  }

  stage_timer = ScopedTimer();
  auto results = BuildResults(counts, customer_layout);
  const double histogram_and_sort_s = stage_timer.ElapsedSeconds();
  const double histogram_s = histogram_and_sort_s;
  const double sort_s = 0.0;
  if (log_timings) {
    std::cout << "[timing] histogram stage complete: elapsed=" << std::fixed
              << std::setprecision(6) << histogram_s
              << "s, cumulative=" << (customer_s + orders_s + histogram_s) << "s\n";
    std::cout << "[timing] sort stage complete: elapsed=" << sort_s
              << "s, cumulative=" << (customer_s + orders_s + histogram_s + sort_s)
              << "s\n";
  }

  stage_timer = ScopedTimer();
  if (out_path.has_value()) {
    WriteResultsCsv(results, *out_path);
  }
  const double write_s = stage_timer.ElapsedSeconds();

  RunOutput output;
  output.results = std::move(results);
  output.timings.customer_scan_s = customer_s;
  output.timings.orders_scan_and_filter_s = orders_s;
  output.timings.histogram_build_s = histogram_s;
  output.timings.final_sort_s = sort_s;
  output.timings.output_write_s = write_s;
  output.timings.processing_total_s = customer_s + orders_s + histogram_s + sort_s;
  output.timings.total_s = total_timer.ElapsedSeconds();

  if (log_timings) {
    std::cout << "[timing] final totals: processing=" << std::fixed
              << std::setprecision(6) << output.timings.processing_total_s
              << "s, write=" << output.timings.output_write_s
              << "s, end_to_end=" << output.timings.total_s << "s\n";
  }

  return output;
}

fs::path ResolveDataDir(const Options& options) {
  if (options.data_dir.has_value()) {
    return *options.data_dir;
  }
  if (!options.sf.has_value()) {
    Fail("Either --data or --sf must be provided.");
  }
  const std::vector<std::string> allowed = {"0.5", "1", "2", "5"};
  if (std::find(allowed.begin(), allowed.end(), *options.sf) == allowed.end()) {
    Fail("Unsupported scale factor '" + *options.sf + "'. Allowed values: 0.5, 1, 2, 5");
  }
  return fs::path("data") / ("sf" + *options.sf);
}

Options ParseArgs(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const std::string& flag) -> std::string {
      if (i + 1 >= argc) {
        Fail("Missing value for " + flag);
      }
      return argv[++i];
    };

    if (arg == "--data") {
      options.data_dir = fs::path(require_value(arg));
    } else if (arg == "--sf") {
      options.sf = require_value(arg);
    } else if (arg == "--out") {
      options.out_path = fs::path(require_value(arg));
    } else if (arg == "--benchmark") {
      options.benchmark_runs = std::stoi(require_value(arg));
    } else if (arg == "--compare-to") {
      options.compare_to = fs::path(require_value(arg));
    } else if (arg == "--log-timings") {
      options.log_timings = true;
    } else if (arg == "--log-every-batches") {
      options.log_every_batches = std::stoi(require_value(arg));
    } else {
      Fail("Unknown argument: " + arg);
    }
  }

  if (options.data_dir.has_value() && options.sf.has_value()) {
    Fail("Use either --data or --sf, not both.");
  }
  if (!options.data_dir.has_value() && !options.sf.has_value()) {
    Fail("Either --data or --sf must be provided.");
  }
  return options;
}

void PrintResults(const std::vector<ResultRow>& rows) {
  std::cout << "c_count,custdist\n";
  for (const auto& row : rows) {
    std::cout << row.c_count << ',' << row.custdist << '\n';
  }
}

void BenchmarkQ13(const fs::path& data_dir,
                  int runs,
                  const std::optional<fs::path>& out_path,
                  bool log_timings,
                  int log_every_batches) {
  if (runs < 2) {
    Fail("Benchmark runs must be at least 2.");
  }

  std::vector<double> totals;
  totals.reserve(static_cast<std::size_t>(runs));

  std::cout << "Benchmarking Q13 on " << data_dir.string() << "\n";
  std::cout << "Runs: " << runs << " (first run treated as warm-up)\n\n";

  for (int i = 0; i < runs; ++i) {
    std::optional<fs::path> effective_out;
    if (i == runs - 1 && out_path.has_value()) {
      effective_out = out_path;
    }

    auto output = RunQ13(data_dir, effective_out, log_timings, log_every_batches);
    totals.push_back(output.timings.total_s);

    std::cout << "Run " << (i + 1)
              << ": total=" << std::fixed << std::setprecision(6) << output.timings.total_s
              << "s, customer=" << output.timings.customer_scan_s
              << "s, orders=" << output.timings.orders_scan_and_filter_s
              << "s, hist=" << output.timings.histogram_build_s
              << "s, sort=" << output.timings.final_sort_s
              << "s, write=" << output.timings.output_write_s << "s\n";
  }

  const double warmup = totals.front();
  const std::vector<double> measured(totals.begin() + 1, totals.end());
  const double average = std::accumulate(measured.begin(), measured.end(), 0.0) /
                         static_cast<double>(measured.size());

  std::vector<double> sorted = measured;
  std::sort(sorted.begin(), sorted.end());
  const double median = sorted[sorted.size() / 2];
  double variance = 0.0;
  for (double value : measured) {
    const double delta = value - average;
    variance += delta * delta;
  }
  variance /= static_cast<double>(measured.size());
  const double stddev = std::sqrt(variance);

  std::cout << "\nSummary\n";
  std::cout << "Warm-up run: " << std::fixed << std::setprecision(6) << warmup << "s\n";
  std::cout << "Average of last " << measured.size() << " runs: " << average << "s\n";
  std::cout << "Median of last " << measured.size() << " runs: " << median << "s\n";
  std::cout << "Std dev of last " << measured.size() << " runs: " << stddev << "s\n";
}

int main(int argc, char** argv) {
  try {
    const auto options = ParseArgs(argc, argv);
    const auto data_dir = ResolveDataDir(options);

    if (options.benchmark_runs > 0) {
      BenchmarkQ13(
          data_dir, options.benchmark_runs, options.out_path, options.log_timings, options.log_every_batches);
    } else {
      auto output = RunQ13(data_dir, options.out_path, options.log_timings, options.log_every_batches);
      PrintResults(output.results);
    }

    if (options.compare_to.has_value() && options.out_path.has_value()) {
      if (CompareCsvOutputs(*options.out_path, *options.compare_to)) {
        std::cout << "Output matches reference CSV.\n";
      } else {
        std::cout << "Output does NOT match reference CSV.\n";
      }
    }

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}