#ifndef Q13_CLI_H
#define Q13_CLI_H

#include <filesystem>
#include <vector>

#include "q13_types.h"

namespace fs = std::filesystem;

fs::path ResolveDataDir(const Options& options);

Options ParseArgs(int argc, char** argv);

void PrintResults(const std::vector<ResultRow>& rows);

void BenchmarkQ13(const fs::path& data_dir,
                  int runs,
                  const std::optional<fs::path>& out_path,
                  bool log_timings,
                  int log_every_batches);

#endif  // Q13_CLI_H
