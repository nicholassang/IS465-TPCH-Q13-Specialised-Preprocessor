#ifndef Q13_UTILS_H
#define Q13_UTILS_H

#include <chrono>
#include <string>
#include <string_view>

#include "arrow/api.h"

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

[[noreturn]] void Fail(const std::string& message);

void CheckStatus(const arrow::Status& status, const std::string& context);

template <typename T>
T ValueOrThrow(arrow::Result<T> result, const std::string& context) {
  if (!result.ok()) {
    Fail(context + ": " + result.status().ToString());
  }
  return std::move(result).ValueUnsafe();
}

std::string Trim(const std::string& value);

bool MatchesSpecialRequests(std::string_view comment);

#endif  // Q13_UTILS_H
