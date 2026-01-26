#pragma once

#include <memory>

#include <boost/asio.hpp>
#include <boost/beast.hpp>

namespace simple_http {
  namespace beast = boost::beast;
  namespace http = beast::http;

  inline std::shared_ptr<http::response<http::string_body>>
  makeHttpResponse(http::status status = http::status::ok, std::string_view content_type = "text/plain") {
    auto res = std::make_shared<http::response<http::string_body>>();
    res->version(11);
    res->result(status);
    res->set(http::field::server, "simple_http_server");
    res->set(http::field::content_type, content_type);
    return res;
  }

}
