#include "q13_engine.h"

#include <iomanip>
#include <iostream>

#include "q13_parquet.h"
#include "q13_processing.h"
#include "q13_utils.h"

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
