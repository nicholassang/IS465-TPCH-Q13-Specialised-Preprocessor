#include "q13_utils.h"

#include <stdexcept>

[[noreturn]] void Fail(const std::string& message) {
  throw std::runtime_error(message);
}

void CheckStatus(const arrow::Status& status, const std::string& context) {
  if (!status.ok()) {
    Fail(context + ": " + status.ToString());
  }
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
