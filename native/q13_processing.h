#ifndef Q13_PROCESSING_H
#define Q13_PROCESSING_H

#include <filesystem>
#include <vector>

#include "arrow/api.h"

#include "q13_types.h"

namespace fs = std::filesystem;

std::vector<std::uint32_t> ProcessOrders(const fs::path& orders_path,
                                         std::size_t counts_size,
                                         const std::vector<std::uint8_t>* valid_customer,
                                         bool log_timings,
                                         int log_every_batches);

std::vector<ResultRow> BuildResults(const std::vector<std::uint32_t>& counts,
                                    const CustomerLayout& customer_layout);

void WriteResultsCsv(const std::vector<ResultRow>& rows, const fs::path& out_path);

bool CompareCsvOutputs(const fs::path& lhs, const fs::path& rhs);

#endif  // Q13_PROCESSING_H
