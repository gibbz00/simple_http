#pragma once

#include <cstdint>

namespace simple_http {
  enum class Version : uint8_t {
    Http1 = 0,
    Http11 = 1,
    Http2 = 2,
  };
}
