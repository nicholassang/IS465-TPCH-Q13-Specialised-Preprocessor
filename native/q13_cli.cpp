#include "q13_cli.h"

#include <algorithm>
#include <iostream>
#include <cmath>
#include <numeric>

#include "q13_engine.h"
#include "q13_utils.h"

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
