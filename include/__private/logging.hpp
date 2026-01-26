#pragma once

#include <cstdint>
#include <format>
#include <functional>
#include <string_view>

namespace simple_http {
  enum class LogLevel : uint8_t {
    Debug,
    Info,
    Error,
  };

  inline constexpr std::string_view to_string(LogLevel level) noexcept {
    switch (level) {
    case LogLevel::Debug:
      return "Debug";
    case LogLevel::Info:
      return "Info";
    case LogLevel::Error:
      return "Error";
    default:
      return "Unknown";
    }
  }

  inline std::function<void(LogLevel, std::string_view, int, std::string)> LOG_CB = [](auto, auto, auto, auto) {};

#define SIMPLE_HTTP_DEBUG_LOG(...) simple_http::log(simple_http::LogLevel::Debug, __FILE__, __LINE__, __VA_ARGS__)
#define SIMPLE_HTTP_INFO_LOG(...) simple_http::log(simple_http::LogLevel::Info, __FILE__, __LINE__, __VA_ARGS__)
#define SIMPLE_HTTP_ERROR_LOG(...) simple_http::log(simple_http::LogLevel::Error, __FILE__, __LINE__, __VA_ARGS__)

  template <typename... Args>
  inline void log(LogLevel level, std::string_view file, int line, std::format_string<Args...> fmt, Args &&...args) {
    LOG_CB(level, file, line, std::format(fmt, std::forward<Args>(args)...));
  }
}
