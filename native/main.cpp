#include <iostream>

#include "q13_cli.h"
#include "q13_engine.h"
#include "q13_processing.h"

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
