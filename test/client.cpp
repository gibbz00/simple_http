#include <boost/asio/io_context.hpp>
#include <chrono>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <variant>

#include <boost/asio.hpp>
#include <boost/beast.hpp>

#include "simple_http/client.hpp"
#include "simple_http/client_utils.hpp"

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

// export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
asio::awaitable<void> client(asio::io_context::executor_type io_context) {
  asio::steady_timer timer(io_context);
  timer.expires_after(std::chrono::seconds(3));
  co_await timer.async_wait();

  std::shared_ptr<simple_http::HttpClient> client =
      std::make_shared<simple_http::HttpsClient>("learnrust.site", 6666, io_context);
  assert(co_await client->start());

  asio::co_spawn(
      io_context,
      [=]() -> asio::awaitable<void> {
        auto req = simple_http::makeHttpRequest("/hello");
        req->set("X-Custom-Header", "value");
        req->body() = "client";
        auto ch = co_await client->sendRequest(req);
        std::cout << "recv http\n";
        for (;;) {
          auto [ec, data] = co_await ch->async_receive(asio::as_tuple(asio::use_awaitable));
          if (ec) {
            std::cout << ec.message() << std::endl;
            break;
          }
          if (std::holds_alternative<simple_http::Disconnect>(data)) {
            std::cout << "Disconnect" << std::endl;
            break;
          } else if (std::holds_alternative<std::string>(data)) {
            auto body_str = std::get<std::string>(data);
            std::cout << "recv body:" << body_str << std::endl;
          } else {
            auto res = std::get<http::response<http::empty_body>>(data);
            for (const auto &field : res) {
              std::cout << field.name_string() << ": " << field.value() << "\n";
            }
          }
        }
      },
      asio::detached);

  timer.expires_after(std::chrono::seconds(30));
  co_await timer.async_wait();

  co_await client->stop();

  co_return;
}

int main() {
  simple_http::LOG_CB = [](simple_http::LogLevel level, auto file, auto line, std::string msg) {
    std::cout << to_string(level) << " " << file << ":" << line << " " << msg << std::endl;
  };

  auto io_context = asio::io_context();

  asio::co_spawn(io_context, client(io_context.get_executor()), asio::detached);

  io_context.run();
}
