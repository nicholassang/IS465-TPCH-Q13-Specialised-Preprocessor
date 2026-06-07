#ifndef Q13_TYPES_H
#define Q13_TYPES_H

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

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

struct CustomerLayout {
  std::vector<std::uint8_t> valid;
  std::size_t max_custkey = 0;
  std::size_t num_customers = 0;
  bool contiguous = false;
};

#endif  // Q13_TYPES_H
