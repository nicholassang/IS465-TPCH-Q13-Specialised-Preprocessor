#ifndef Q13_ENGINE_H
#define Q13_ENGINE_H

#include <filesystem>
#include <optional>

#include "q13_types.h"

namespace fs = std::filesystem;

RunOutput RunQ13(const fs::path& data_dir,
                 const std::optional<fs::path>& out_path,
                 bool log_timings,
                 int log_every_batches);

#endif  // Q13_ENGINE_H
