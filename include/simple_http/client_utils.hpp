#pragma once

#include <memory>

#include <boost/asio.hpp>
#include <boost/beast.hpp>

#include "common.hpp"

namespace simple_http {
  namespace beast = boost::beast;
  namespace http = beast::http;

  inline std::shared_ptr<http::request<http::string_body>> makeHttpRequest(const std::string &path,
                                                                           http::verb method = http::verb::post,
                                                                           Version http_version = Version::Http11) {
    auto req =
        std::make_shared<http::request<http::string_body>>(method, path, http_version == Version::Http11 ? 11 : 10);
    req->set(http::field::user_agent, "simpe_http_client");
    return req;
  }
}
