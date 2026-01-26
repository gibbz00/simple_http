#pragma once

#include <cstdint>

#include <boost/asio.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <nghttp2/nghttp2.h>

namespace simple_http {
  namespace asio = boost::asio;

  constexpr int32_t CHANNEL_SIZE = 100000;

  struct Disconnect {};

  using Http2Channel = asio::experimental::concurrent_channel<void(
      boost::system::error_code, std::variant<std::shared_ptr<std::string>, Disconnect>)>;

  struct DataContext {
      const char *data;
      size_t total_len;
      size_t offset;
      std::shared_ptr<void> input_data{nullptr};
  };

  inline ssize_t dataReadCallback(nghttp2_session * /* session */,
                                  int32_t /* stream_id */,
                                  uint8_t *buf,
                                  size_t length,
                                  uint32_t *data_flags,
                                  nghttp2_data_source *source,
                                  void * /* user_data */) {
    auto *ctx = static_cast<DataContext *>(source->ptr);

    size_t remaining = ctx->total_len - ctx->offset;
    size_t to_copy = remaining < length ? remaining : length;

    memcpy(buf, ctx->data + ctx->offset, to_copy);
    ctx->offset += to_copy;

    if (ctx->offset >= ctx->total_len) {
      *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      delete ctx;
    }

    return to_copy;
  }

  inline asio::awaitable<void> toSocket(auto socket,
                                        std::shared_ptr<Http2Channel> ch,
                                        std::shared_ptr<std::chrono::steady_clock::time_point> deadline,
                                        std::chrono::seconds max_idle_time) {
    std::vector<std::shared_ptr<std::string>> vec;
    bool force_close = false;
    for (;;) {
      *deadline = std::chrono::steady_clock::now() + max_idle_time;
      while (true) {
        std::variant<std::shared_ptr<std::string>, Disconnect> data;
        if (!ch->try_receive([&](auto, auto recv_data) { data = std::move(recv_data); })) {
          break;
        }
        if (std::holds_alternative<Disconnect>(data)) {
          force_close = true;
          break;
        } else {
          auto &info_ptr = std::get<std::shared_ptr<std::string>>(data);
          vec.emplace_back(std::move(info_ptr));
        }
      }

      if (vec.empty() && !force_close) {
        std::variant<std::shared_ptr<std::string>, Disconnect> data;

        boost::system::error_code ec;
        std::tie(ec, data) = co_await ch->async_receive(asio::as_tuple(asio::use_awaitable));
        if (ec) {
          break;
        }
        if (std::holds_alternative<Disconnect>(data)) {
          force_close = true;
        } else {
          auto &info_ptr = std::get<std::shared_ptr<std::string>>(data);
          vec.emplace_back(std::move(info_ptr));
        }
        *deadline = std::chrono::steady_clock::now();
      }

      if (!vec.empty()) {
        std::vector<asio::const_buffer> buffers;
        buffers.reserve(vec.size());
        for (const auto &s : vec) {
          buffers.push_back(asio::buffer(*s));
        }
        if (auto [ec, nwritten] = co_await async_write(*socket, buffers, asio::as_tuple(asio::use_awaitable)); ec) {
          break;
        }
        vec.clear();
      }

      if (force_close)
        break;
    }
    co_return;
  }

  inline asio::awaitable<void> toH2Parse(auto socket,
                                         auto h2p,
                                         std::shared_ptr<std::chrono::steady_clock::time_point> deadline,
                                         std::chrono::seconds max_idle_time) {
    char buffer[4096];
    for (;;) {
      *deadline = std::chrono::steady_clock::now() + max_idle_time;
      auto [ec, nread] =
          co_await socket->async_read_some(asio::buffer(buffer, sizeof(buffer)), asio::as_tuple(asio::use_awaitable));
      if (ec) {
        break;
      }
      auto ret = h2p->feedRecvData(buffer, nread);
      if (ret == -1) {
        break;
      }
    }
  };

  inline asio::awaitable<void> watchdog(std::shared_ptr<std::chrono::steady_clock::time_point> deadline) {
    asio::steady_timer timer(co_await asio::this_coro::executor);

    auto now = std::chrono::steady_clock::now();
    while (*deadline > now) {
      timer.expires_at(*deadline);
      co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
      now = std::chrono::steady_clock::now();
    }
    co_return;
  }

  void shutdown(const auto &socket) {
    if constexpr (std::is_same_v<std::shared_ptr<asio::ip::tcp::socket>, std::decay_t<decltype(socket)>>) {
      if (socket->is_open()) {
        boost::system::error_code ec;
        socket->shutdown(asio::ip::tcp::socket::shutdown_both, ec);
        socket->close(ec);
      }
    } else {
      boost::system::error_code ec;
      socket->shutdown(ec);
      socket->next_layer().shutdown(asio::ip::tcp::socket::shutdown_both, ec);
      socket->next_layer().close(ec);
    }
  }
}
