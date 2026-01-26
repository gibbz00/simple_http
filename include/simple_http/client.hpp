#pragma once

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <nghttp2/nghttp2.h>

#include "__private/common.hpp"
#include "__private/logging.hpp"

namespace simple_http {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;

  using namespace asio::experimental::awaitable_operators;

  using error_code = boost::system::error_code;

  struct HttpClient {
      using Channel = asio::experimental::concurrent_channel<void(
          boost::system::error_code, std::variant<http::response<http::empty_body>, std::string, Disconnect>)>;

      using ReqChannel = asio::experimental::concurrent_channel<void(
          error_code, std::tuple<std::shared_ptr<http::request<http::string_body>>, std::shared_ptr<Channel>>)>;

      HttpClient() = default;
      virtual ~HttpClient() = default;

      virtual asio::awaitable<bool> start() = 0;
      virtual asio::awaitable<void> stop() = 0;
      virtual asio::awaitable<std::shared_ptr<Channel>>
      sendRequest(std::shared_ptr<http::request<http::string_body>> req) = 0;
  };

  class HttpsClient final : public HttpClient, public std::enable_shared_from_this<HttpsClient> {
    public:
      HttpsClient(std::string host, uint16_t port, asio::io_context::executor_type io_context, int32_t timeout = 60)
          : m_host(std::move(host)), m_port(port), m_io_context(io_context), m_timeout(timeout),
            m_ssl_context(asio::ssl::context::tlsv13_client) {
        m_h2_channel = std::make_shared<Http2Channel>(m_io_context, CHANNEL_SIZE);
        m_req_channel = std::make_shared<ReqChannel>(m_io_context, CHANNEL_SIZE);

        m_ssl_context.set_verify_mode(SSL_VERIFY_PEER);
        m_ssl_context.set_default_verify_paths();
        const unsigned char alpn_protos[] = {0x02, 'h', '2'};
        SSL_CTX_set_alpn_protos(m_ssl_context.native_handle(), alpn_protos, sizeof(alpn_protos));
      }

      asio::awaitable<bool> start() override {
        co_await asio::dispatch(asio::bind_executor(m_io_context, asio::use_awaitable));

        if (m_connected)
          co_return true;

        auto solver = asio::ip::tcp::resolver(m_io_context);
        auto [ec, results] =
            co_await solver.async_resolve(m_host, std::to_string(m_port), asio::as_tuple(asio::use_awaitable));
        if (ec) {
          SIMPLE_HTTP_ERROR_LOG("async_resolve: {}", ec.message());
          co_return false;
        }

        asio::ip::tcp::socket socket(m_io_context);
        asio::steady_timer timer(m_io_context);
        timer.expires_after(std::chrono::seconds(10));
        auto result = co_await (socket.async_connect(*(results.begin()), asio::as_tuple(asio::use_awaitable)) ||
                                timer.async_wait(asio::as_tuple(asio::use_awaitable)));
        if (result.index() == 0) {
          auto [ec] = std::get<0>(result);
          if (ec) {
            SIMPLE_HTTP_ERROR_LOG("async_connect: {}", ec.message());
            co_return false;
          }
        } else if (result.index() == 1) {
          SIMPLE_HTTP_ERROR_LOG("async_connect timeout");
          co_return false;
        }

        m_socket = std::make_unique<asio::ssl::stream<asio::ip::tcp::socket>>(std::move(socket), m_ssl_context);

        if (!SSL_set_tlsext_host_name(m_socket->native_handle(), m_host.c_str())) {
          ec = boost::system::error_code(static_cast<int>(::ERR_get_error()), asio::error::get_ssl_category());
          SIMPLE_HTTP_ERROR_LOG("SSL_set_tlsext_host_name: {}", ec.message());
          co_return false;
        }

        if (auto [ec] =
                co_await m_socket->async_handshake(asio::ssl::stream_base::client, asio::as_tuple(asio::use_awaitable));
            ec) {
          SIMPLE_HTTP_ERROR_LOG("async_handshake: {}", ec.message());
          co_return false;
        }
        const unsigned char *protocol = nullptr;
        unsigned int length = 0;

        SSL_get0_alpn_selected(m_socket->native_handle(), &protocol, &length);

        if (length == 2 && std::memcmp(protocol, "h2", 2) == 0) {
          SIMPLE_HTTP_INFO_LOG("Negotiated ALPN: h2");
          m_h2 = true;
          initNghttp2();
          co_await startHttp2Client();
          m_connected = true;
        } else {
          SIMPLE_HTTP_INFO_LOG("ALPN negotiation failed or not h2.");
          m_h2 = false;
        }

        co_return true;
      }

      asio::awaitable<void> stop() override {
        co_await asio::dispatch(asio::bind_executor(m_io_context, asio::use_awaitable));
        if (m_socket) {
          shutdown(m_socket);
        }
        if (m_session) {
          nghttp2_session_callbacks_del(m_cbs);
          nghttp2_session_del(m_session);
          m_session = nullptr;
        }
        co_return;
      }

      int initNghttp2() {
        if (m_session) {
          nghttp2_session_callbacks_del(m_cbs);
          nghttp2_session_del(m_session);
          m_session = nullptr;
        }
        nghttp2_session_callbacks_new(&m_cbs);
        nghttp2_session_callbacks_set_on_header_callback(m_cbs, onHeaderCallback);
        nghttp2_session_callbacks_set_send_callback(m_cbs, sendCallback);
        nghttp2_session_callbacks_set_on_frame_recv_callback(m_cbs, onFrameRecvCallback);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(m_cbs, onDataChunkRecvCallback);

        nghttp2_session_client_new(&m_session, m_cbs, this);

        std::vector<nghttp2_settings_entry> iv;
        iv.emplace_back(NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 200);

        nghttp2_submit_settings(m_session, NGHTTP2_FLAG_NONE, iv.data(), iv.size());
        nghttp2_session_send(m_session);

        return 0;
      }

      static int onHeaderCallback(nghttp2_session * /* session */,
                                  const nghttp2_frame *frame,
                                  const uint8_t *_name,
                                  size_t namelen,
                                  const uint8_t *_value,
                                  size_t valuelen,
                                  uint8_t /* flags */,
                                  void *userdata) {
        int32_t stream_id = frame->hd.stream_id;
        auto cli = static_cast<HttpsClient *>(userdata);
        if (cli->m_streams.contains(stream_id)) {
          std::string name{(char *)_name, namelen};
          std::string_view value{(char *)_value, valuelen};
          std::ranges::transform(name, name.begin(), [](unsigned char c) { return std::tolower(c); });
          if (name == ":status") {
            cli->m_streams[stream_id]->header.result(std::stoul(value.data()));
          } else {
            cli->m_streams[stream_id]->header.set(name, value);
          }
        } else {
          SIMPLE_HTTP_ERROR_LOG("not found : {}", stream_id);
        }
        return 0;
      }

      static ssize_t sendCallback(
          nghttp2_session * /* session */, const uint8_t *data, size_t length, int /* flags */, void *userdata) {
        auto h2_cli = static_cast<HttpsClient *>(userdata);
        if (!h2_cli->m_h2_channel->try_send(error_code{}, std::make_shared<std::string>((char *)data, length))) {
          SIMPLE_HTTP_ERROR_LOG("sendCallback send error!!!!");
        }
        return length;
      }

      static int onFrameRecvCallback(nghttp2_session * /* session */, const nghttp2_frame *frame, void *userdata) {
        auto call_handler = [&] {
          int32_t stream_id = frame->hd.stream_id;
          auto h2_cli = static_cast<HttpsClient *>(userdata);
          if (h2_cli->m_streams.contains(stream_id)) {
            h2_cli->m_streams[stream_id]->close();
          } else {
            SIMPLE_HTTP_ERROR_LOG("not found : {}", stream_id);
          }
          h2_cli->m_streams.erase(stream_id);
        };

        if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_RESPONSE) {
          int32_t stream_id = frame->hd.stream_id;
          auto h2_cli = static_cast<HttpsClient *>(userdata);
          if (h2_cli->m_streams.contains(stream_id)) {
            h2_cli->m_streams[stream_id]->sendHttpHeader();
          }

          if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
            call_handler();
          }
        }

        if (frame->hd.type == NGHTTP2_DATA && (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
          call_handler();
        }

        return 0;
      }

      static int onDataChunkRecvCallback(nghttp2_session * /* session */,
                                         uint8_t /* flags */,
                                         int32_t stream_id,
                                         const uint8_t *data,
                                         size_t len,
                                         void *userdata) {
        auto h2_cli = static_cast<HttpsClient *>(userdata);
        if (h2_cli->m_streams.contains(stream_id)) {
          h2_cli->m_streams[stream_id]->sendHttpBody(std::string{(char *)data, len});
        } else {
          SIMPLE_HTTP_ERROR_LOG("not found : {}", stream_id);
        }
        return 0;
      }

      asio::awaitable<std::shared_ptr<Channel>>
      sendRequest(std::shared_ptr<http::request<http::string_body>> req) override {
        auto channel = std::make_shared<Channel>(m_io_context, CHANNEL_SIZE);
        auto tp = std::make_tuple(std::move(req), channel);
        if (!m_req_channel->try_send(error_code{}, tp)) {
          auto [ec] = co_await m_req_channel->async_send(error_code{}, tp, asio::as_tuple(asio::use_awaitable));
          if (ec) {
            co_return nullptr;
          }
        }
        co_return channel;
      }

      int feedRecvData(const char *data, size_t len) {
        size_t ret = nghttp2_session_mem_recv(m_session, (const uint8_t *)data, len);
        if (ret != len) {
          SIMPLE_HTTP_ERROR_LOG("nghttp2 error: {}", nghttp2_strerror(ret));
          return -1;
        }
        return (int)ret;
      }

      bool connected() { return m_connected.load(std::memory_order_relaxed); }

    private:
      asio::awaitable<void> forwardRequest() {
        auto submit_request = [this](auto tp) mutable {
          auto &[req, ch] = tp;
          std::vector<nghttp2_nv> hdrs;
          auto fill = [](std::string_view name, std::string_view value, auto &hdrs) {
            nghttp2_nv nv;
            nv.name = (uint8_t *)name.data();
            nv.namelen = name.size();
            nv.value = (uint8_t *)value.data();
            nv.valuelen = value.size();
            nv.flags = NGHTTP2_NV_FLAG_NONE;
            hdrs.push_back(nv);
          };

          fill(":path", req->target(), hdrs);
          fill(":scheme", "https", hdrs);
          fill(":authority", m_host, hdrs);
          std::string method_str = http::to_string(req->method());
          fill(":method", method_str, hdrs);

          for (const auto &field : *req) {
            fill(field.name_string(), field.value(), hdrs);
          }

          const auto &post_data = req->body();

          nghttp2_data_provider data_prd;
          auto *ctx =
              new DataContext{.data = post_data.data(), .total_len = post_data.size(), .offset = 0, .input_data = req};
          data_prd.source.ptr = ctx;
          data_prd.read_callback = dataReadCallback;
          int stream_id = nghttp2_submit_request(m_session, nullptr, hdrs.data(), hdrs.size(), &data_prd, nullptr);

          nghttp2_session_send(m_session);
          if (stream_id < 0) {
            SIMPLE_HTTP_ERROR_LOG("Failed to submit POST request: {}", nghttp2_strerror(stream_id));
            ch->close();
            return -1;
          }
          m_streams.emplace(stream_id, std::make_shared<Response>(std::move(ch)));
          return 0;
        };
        for (;;) {
          auto [ec, tp] = co_await m_req_channel->async_receive(asio::as_tuple(asio::use_awaitable));
          if (ec) {
            break;
          }
          auto [req, ch] = tp;
          submit_request(std::move(tp));
        }
      }

      asio::awaitable<void> startHttp2Client() {
        auto func =
            [](auto socket, auto h2_channel, std::weak_ptr<HttpsClient> self, auto timeout) -> asio::awaitable<void> {
          auto sp = self.lock();
          auto deadline = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
          co_await (toH2Parse(socket, sp, deadline, std::chrono::seconds(timeout)) ||
                    toSocket(socket, h2_channel, deadline, std::chrono::seconds(timeout)) || sp->forwardRequest() ||
                    watchdog(deadline));
          sp->m_connected = false;
          // disconnect
          for (auto &[id, rsp] : sp->m_streams) {
            rsp->m_channel->try_send(error_code{}, Disconnect{});
          }
          sp->m_streams.clear();
        };
        asio::co_spawn(m_io_context, func(m_socket, m_h2_channel, shared_from_this(), m_timeout), asio::detached);
        co_return;
      }

      std::string m_host;
      uint16_t m_port;
      asio::io_context::executor_type m_io_context;
      int32_t m_timeout;

      asio::ssl::context m_ssl_context;
      std::shared_ptr<asio::ssl::stream<asio::ip::tcp::socket>> m_socket;
      bool m_h2;
      nghttp2_session_callbacks *m_cbs{};
      nghttp2_session *m_session{};
      std::shared_ptr<Http2Channel> m_h2_channel;
      std::shared_ptr<ReqChannel> m_req_channel;
      std::atomic_bool m_connected{false};

      struct Response {
          Response(std::shared_ptr<Channel> ch) : m_channel(std::move(ch)) {}

          bool sendHttpHeader() { return m_channel->try_send(error_code{}, std::move(header)); }

          bool sendHttpBody(std::string body) { return m_channel->try_send(error_code{}, std::move(body)); }

          void close() {
            if (m_channel)
              m_channel->close();
          }

          http::response<http::empty_body> header;
          std::shared_ptr<Channel> m_channel;
      };

      std::unordered_map<int32_t, std::shared_ptr<Response>> m_streams;
  };
}
