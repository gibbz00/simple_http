#pragma once

#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <tuple>
#include <type_traits>
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

#include "common.hpp"

#include "__private/base64.hpp"
#include "__private/common.hpp"
#include "__private/logging.hpp"

namespace simple_http {
  namespace asio = boost::asio;
  namespace beast = boost::beast;
  namespace http = beast::http;

  using error_code = boost::system::error_code;

  using namespace boost::asio::experimental::awaitable_operators;

  inline bool isHttp2(const std::string &cache_data) {
    if (cache_data.empty() || cache_data.size() < 6)
      return false;
    if (cache_data[0] == 0x50 && cache_data[1] == 0x52 && cache_data[2] == 0x49 && cache_data[3] == 0x20 &&
        cache_data[4] == 0x2A && cache_data[5] == 0x20) {
      return true;
    } else {
      return false;
    }
  }

  class HttpResponseWriter;

  using Http1Channel = asio::experimental::concurrent_channel<void(
      error_code, std::variant<std::shared_ptr<http::response<http::string_body>>, std::string, Disconnect>)>;

  struct HandlerFunctions {
      void setBefore(std::function<asio::awaitable<bool>(const http::request<http::string_body> &,
                                                         const std::shared_ptr<HttpResponseWriter> &)> cb) {
        before = std::move(cb);
      }

      void setHttpHandler(const std::string &path,
                          std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                              std::shared_ptr<HttpResponseWriter>)> cb) {
        map_proc[path] = std::move(cb);
      }

      void setHttpRegexHandler(const std::string &regex,
                               std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                                   std::shared_ptr<HttpResponseWriter>)> cb) {
        try {
          regex_proc.emplace_back(std::regex{regex}, std::move(cb));
        } catch (const std::exception &e) {
          SIMPLE_HTTP_ERROR_LOG("regex:[{}], {}", regex, e.what());
        }
      }

      void setUnhandled(std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                            std::shared_ptr<HttpResponseWriter>)> cb) {
        map_proc["*"] = std::move(cb);
      }

      std::function<asio::awaitable<bool>(const http::request<http::string_body> &,
                                          const std::shared_ptr<HttpResponseWriter> &)>
          before{[](const auto &, const auto &) -> asio::awaitable<bool> { co_return true; }};

      std::unordered_map<
          std::string,
          std::function<asio::awaitable<void>(http::request<http::string_body>, std::shared_ptr<HttpResponseWriter>)>>
          map_proc{{"*", [](auto, auto writer) -> asio::awaitable<void> {
                      if (writer->version() == simple_http::Version::Http2) {
                        writer->writeStatus(404);
                        writer->writeHeader("content-type", "text/plain");
                        writer->writeHeader(http::field::server, "simple_http");
                        writer->writeHeaderEnd();
                        writer->writeBodyEnd("");
                      } else {
                        http::response<http::string_body> res{http::status::not_found, 11};
                        res.set(http::field::content_type, "text/plain");
                        res.set(http::field::server, "simple_http");
                        res.body() = "";
                        res.prepare_payload();
                        writer->writeHttpResponse(std::make_shared<http::response<http::string_body>>(res));
                      }
                      co_return;
                    }}};
      std::vector<std::pair<
          std::regex,
          std::function<asio::awaitable<void>(http::request<http::string_body>, std::shared_ptr<HttpResponseWriter>)>>>
          regex_proc;
  };

  asio::awaitable<void> callCallback(std::weak_ptr<HandlerFunctions> hf, auto req, auto writer) {
    auto sp = hf.lock();
    if (!sp)
      co_return;
    if (!co_await sp->before(req, writer)) {
      co_return;
    }
    std::string path = req.target();
    if (auto pos = path.find('?'); pos != std::string::npos) {
      path = path.substr(0, pos);
    }
    if (sp->map_proc.contains(path)) {
      co_await sp->map_proc[path](std::move(req), std::move(writer));
    } else {
      for (const auto &[pattern, cb] : sp->regex_proc) {
        if (std::regex_match(path, pattern)) {
          co_await cb(std::move(req), std::move(writer));
          co_return;
        }
      }
      co_await sp->map_proc["*"](std::move(req), std::move(writer));
    }
    co_return;
  }

  inline void callHandler(const std::weak_ptr<HandlerFunctions> &hf,
                          asio::io_context::executor_type io_context,
                          auto req,
                          auto writer) {
    asio::co_spawn(io_context, callCallback(hf, std::move(req), std::move(writer)), [](const std::exception_ptr &ep) {
      try {
        if (ep)
          std::rethrow_exception(ep);
      } catch (const std::exception &e) {
        SIMPLE_HTTP_ERROR_LOG("{}", e.what());
      } catch (...) {
        SIMPLE_HTTP_ERROR_LOG("unknown exception");
      }
    });
    return;
  }

  class Http2Parse final : public std::enable_shared_from_this<Http2Parse> {
    public:
      struct Config {
          bool is_h2c_upgrade;
          std::string h2_setting;
          int32_t concurrent_streams{200};
          std::optional<int32_t> window_size;
          std::optional<int32_t> max_frame_size;
          http::verb method;
      };

      Http2Parse(const std::shared_ptr<Http2Channel> &ch2,
                 const std::shared_ptr<Http1Channel> &ch1,
                 asio::io_context::executor_type io_context,
                 const std::shared_ptr<HandlerFunctions> &handler_functions)
          : m_h2_channel(ch2), m_h1_channel(ch1), m_io_context(io_context), m_handler_functions(handler_functions) {}

      ~Http2Parse() {
        if (m_session) {
          nghttp2_session_callbacks_del(m_cbs);
          nghttp2_session_del(m_session);
        }
      }

      int init(const Config &cfg) {
        nghttp2_session_callbacks_new(&m_cbs);
        nghttp2_session_callbacks_set_on_header_callback(m_cbs, onHeaderCallback);
        nghttp2_session_callbacks_set_send_callback(m_cbs, sendCallback);
        nghttp2_session_callbacks_set_on_frame_recv_callback(m_cbs, onFrameRecvCallback);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(m_cbs, onDataChunkRecvCallback);

        nghttp2_session_server_new(&m_session, m_cbs, this);

        if (cfg.is_h2c_upgrade) {
          auto http2_settings_base64 = base64_decode(cfg.h2_setting);
          auto ret = nghttp2_session_upgrade2(m_session,
                                              (uint8_t *)http2_settings_base64.data(),
                                              http2_settings_base64.size(),
                                              cfg.method == http::verb::head ? 1 : 0,
                                              nullptr);
          if (ret) {
            SIMPLE_HTTP_ERROR_LOG("nghttp2_session_upgrade2 error: {}", nghttp2_strerror(ret));
            return ret;
          }
        }

        std::vector<nghttp2_settings_entry> iv;
        iv.emplace_back(NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, cfg.concurrent_streams);
        if (cfg.window_size.has_value()) {
          iv.emplace_back(NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, cfg.window_size.value());
        }
        if (cfg.max_frame_size.has_value()) {
          iv.emplace_back(NGHTTP2_SETTINGS_MAX_FRAME_SIZE, cfg.max_frame_size.value());
        }
        nghttp2_submit_settings(m_session, NGHTTP2_FLAG_NONE, iv.data(), iv.size());
        nghttp2_session_send(m_session);

        return 0;
      }

      bool
      writeHeaderEnd(std::unordered_map<std::string, std::string> headers, int32_t stream_id, std::string http_status) {
        if (auto sp = m_h2_channel.lock()) {
          std::weak_ptr<Http2Parse> self = shared_from_this();
          asio::post(
              sp->get_executor(),
              [this,
               self = std::move(self),
               headers = std::move(headers),
               stream_id,
               http_status = std::move(http_status)]() mutable {
                if (auto sp = self.lock()) {
                  std::vector<nghttp2_nv> hdrs;
                  auto fill = [](const auto &name, const auto &value, auto &hdrs) {
                    nghttp2_nv nv;
                    nv.name = (uint8_t *)name.c_str();
                    nv.namelen = name.size();
                    nv.value = (uint8_t *)value.c_str();
                    nv.valuelen = value.size();
                    nv.flags = NGHTTP2_NV_FLAG_NONE;
                    hdrs.push_back(nv);
                  };
                  static std::string status{":status"};
                  fill(status, http_status, hdrs);
                  for (auto &[name, value] : headers) {
                    fill(name, value, hdrs);
                  }
                  nghttp2_submit_headers(
                      m_session, NGHTTP2_FLAG_END_HEADERS, stream_id, nullptr, hdrs.data(), hdrs.size(), nullptr);
                  nghttp2_session_send(m_session);
                }
              });
          return true;
        }
        return false;
      }

      bool writeBody(std::string data, int32_t stream_id, nghttp2_flag flag = NGHTTP2_FLAG_NONE) {
        if (auto sp = m_h2_channel.lock()) {
          std::weak_ptr<Http2Parse> self = shared_from_this();
          asio::post(
              sp->get_executor(),
              [this, self = std::move(self), data = std::make_shared<std::string>(std::move(data)), flag, stream_id] {
                if (auto sp = self.lock()) {
                  nghttp2_data_provider data_prd;
                  data_prd.read_callback = dataReadCallback;
                  auto *ctx = new DataContext{
                      .data = data->c_str(), .total_len = data->size(), .offset = 0, .input_data = data};
                  nghttp2_data_source source;
                  source.ptr = ctx;
                  data_prd.source = source;
                  nghttp2_submit_data(m_session, flag, stream_id, &data_prd);
                  nghttp2_session_send(m_session);
                }
              });
          return true;
        }
        return false;
      }

      bool writeChunkData(std::string data) {
        if (auto sp = m_h1_channel.lock()) {
          if (!sp->try_send(error_code{}, std::move(data))) {
            SIMPLE_HTTP_ERROR_LOG("writeChunkData error");
            return false;
          }
          return true;
        }
        return false;
      }

      bool writeHttp1Response(const std::shared_ptr<http::response<http::string_body>> &http_1_response) {
        if (auto sp = m_h1_channel.lock()) {
          if (!sp->try_send(error_code{}, http_1_response)) {
            SIMPLE_HTTP_ERROR_LOG("{}", "writeHttp1Response error");
            return false;
          }
          return true;
        }
        SIMPLE_HTTP_DEBUG_LOG("{}", "writeHttp1Response client disconnect!!!");
        return false;
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
        auto h2p = static_cast<Http2Parse *>(userdata);
        auto &req = h2p->getStreamCtx(stream_id);
        std::string name{(char *)_name, namelen};
        std::string_view value{(char *)_value, valuelen};
        std::ranges::transform(name, name.begin(), [](unsigned char c) { return std::tolower(c); });
        if (name == ":method") {
          req->method(http::string_to_verb(value));
        } else if (name == ":path") {
          req->target(value);
        } else {
          req->set(name, value);
        }
        return 0;
      }

      static ssize_t sendCallback(
          nghttp2_session * /* session */, const uint8_t *data, size_t length, int /* flags */, void *userdata) {
        auto h2p = static_cast<Http2Parse *>(userdata);
        auto sp = h2p->m_h2_channel.lock();
        if (sp == nullptr) {
          SIMPLE_HTTP_DEBUG_LOG("{}", "sendCallback client disconnect!!!");
          return length;
        }
        if (!sp->try_send(error_code{}, std::make_shared<std::string>((char *)data, length))) {
          SIMPLE_HTTP_ERROR_LOG("{}", "sendCallback send error!!!!");
        }
        return length;
      }

      static int onFrameRecvCallback(nghttp2_session * /* session */, const nghttp2_frame *frame, void *userdata) {
        auto call_handler = [&] {
          int32_t stream_id = frame->hd.stream_id;
          auto h2p = static_cast<Http2Parse *>(userdata);
          auto &req = h2p->getStreamCtx(stream_id);
          req->prepare_payload();
          auto writer = std::make_shared<HttpResponseWriter>(h2p->shared_from_this(), stream_id, Version::Http2);
          if (auto sp = h2p->m_h2_channel.lock()) {
            callHandler(h2p->m_handler_functions, h2p->m_io_context, std::move(*req), std::move(writer));
          }
          h2p->erase(stream_id);
        };

        if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
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
        auto h2p = static_cast<Http2Parse *>(userdata);
        auto &req = h2p->getStreamCtx(stream_id);
        req->body().append((char *)data, len);
        return 0;
      }

      int feedRecvData(const char *data, size_t len) {
        size_t ret = nghttp2_session_mem_recv(m_session, (const uint8_t *)data, len);
        if (ret != len) {
          SIMPLE_HTTP_ERROR_LOG("nghttp2 error: {}", nghttp2_strerror(ret));
          return -1;
        }
        return (int)ret;
      }

      std::shared_ptr<http::request<http::string_body>> &getStreamCtx(int32_t stream_id) {
        if (!m_streams.contains(stream_id)) {
          m_streams[stream_id] = std::make_shared<http::request<http::string_body>>();
        }
        return m_streams[stream_id];
      }

      void erase(int32_t stream_id) { m_streams.erase(stream_id); }

      std::weak_ptr<Http2Channel> m_h2_channel;
      std::weak_ptr<Http1Channel> m_h1_channel;
      asio::io_context::executor_type m_io_context;
      nghttp2_session_callbacks *m_cbs{};
      nghttp2_session *m_session{};
      std::unordered_map<int32_t, std::shared_ptr<http::request<http::string_body>>> m_streams;
      std::weak_ptr<HandlerFunctions> m_handler_functions;
  };

  class HttpResponseWriter {
    public:
      HttpResponseWriter(const std::shared_ptr<Http2Parse> &http2_helper, int32_t stream_id, Version version)
          : m_http2_parse(http2_helper), m_stream_id(stream_id), m_version(version) {}

      ~HttpResponseWriter() {
        if (m_version == Version::Http2 && !m_write_h2_header_done) {
          writeHeaderEnd();
        }
        if (m_version == Version::Http2 && !m_write_h2_body_done) {
          writeBodyEnd("");
        }
      }

      void writeStatus(int32_t http_status) { m_http_status = std::to_string(http_status); }

      void writeStatus(http::status http_status) { writeStatus(static_cast<int32_t>(http_status)); }

      template <typename Key, typename Value> void writeHeader(Key &&key, Value &&value) {
        if constexpr (std::is_same_v<std::decay_t<Key>, http::field>) {
          m_headers.emplace(http::to_string(std::forward<Key>(key)), std::forward<Value>(value));
        } else {
          m_headers.emplace(std::forward<Key>(key), std::forward<Value>(value));
        }
      }

      void writeHeader(std::unordered_map<std::string, std::string> headers) { m_headers.merge(headers); }

      bool writeHeaderEnd() {
        if (m_version != Version::Http2)
          return false;
        static std::string server = http::to_string(http::field::server);
        if (!m_headers.contains(server)) {
          m_headers.emplace(server, "simple_http_server");
        }
        m_write_h2_header_done = true;
        return m_http2_parse->writeHeaderEnd(std::move(m_headers), m_stream_id, std::move(m_http_status));
      }

      template <typename T> bool writeBody(T &&data, nghttp2_flag flag = NGHTTP2_FLAG_NONE) {
        if (m_version != Version::Http2)
          return false;
        static_assert(std::is_constructible_v<std::string, T &&>, "T must be convertible to std::string");
        return m_http2_parse->writeBody(std::forward<T>(data), m_stream_id, flag);
      }

      template <typename T> bool writeBodyEnd(T &&data) {
        if (m_version != Version::Http2)
          return false;
        m_write_h2_body_done = true;
        return writeBody(std::forward<T>(data), NGHTTP2_FLAG_END_STREAM);
      }

      // for http1.1 chunk
      bool writeChunkHeader(const http::response<http::empty_body> &res) {
        std::stringstream ss;
        ss << res.base();
        return m_http2_parse->writeChunkData(ss.str());
      }

      bool writeChunkData(const char *ptr, size_t size) {
        constexpr size_t max_chunk_header_len = 20;
        char buffer[max_chunk_header_len];
        int len = snprintf(buffer, max_chunk_header_len, "%zx\r\n", size);
        std::string chunk;
        chunk.reserve(len + size + 2 + 2);
        chunk.append(buffer, len);
        chunk.append(ptr, size);
        chunk.append("\r\n", 2);
        return m_http2_parse->writeChunkData(std::move(chunk));
      }

      bool writeChunkData(const std::string &data) { return writeChunkData(data.c_str(), data.size()); }

      bool writeChunkEnd() {
        static std::string close_stream{"0\r\n\r\n"};
        return m_http2_parse->writeChunkData(close_stream);
      }

      bool connected() {
        if (m_version == Version::Http2) {
          return m_http2_parse->m_h2_channel.lock() ? true : false;
        } else {
          return m_http2_parse->m_h1_channel.lock() ? true : false;
        }
      }

      void forceClose() {
        if (m_version == Version::Http2) {
          if (auto sp = m_http2_parse->m_h2_channel.lock()) {
            asio::post(sp->get_executor(), [sp] { sp->try_send(error_code{}, Disconnect{}); });
          }
        } else {
          if (auto sp = m_http2_parse->m_h1_channel.lock()) {
            asio::post(sp->get_executor(), [sp] { sp->try_send(error_code{}, Disconnect{}); });
          }
        }
      }

      bool writeHttpResponse(const std::shared_ptr<http::response<http::string_body>> &http_response) {
        if (m_version == Version::Http2) {
          m_http_status = std::to_string(http_response->result_int());
          for (const auto &field : http_response->base()) {
            writeHeader(field.name_string(), field.value());
          }
          writeHeaderEnd();
          writeBodyEnd(std::move(http_response->body()));
          return true;
        } else {
          return m_http2_parse->writeHttp1Response(http_response);
        }
      }

      auto version() { return m_version; }

    private:
      std::shared_ptr<Http2Parse> m_http2_parse;
      int32_t m_stream_id;
      Version m_version;
      std::string m_http_status{"200"};
      std::unordered_map<std::string, std::string> m_headers;
      bool m_write_h2_header_done{false};
      bool m_write_h2_body_done{false};
  };

  struct Config {
      std::string ip;
      uint16_t port;
      uint16_t worker_num{4};
      int32_t concurrent_streams{200};
      std::optional<int32_t> window_size;
      std::optional<int32_t> max_frame_size;
      std::chrono::seconds max_idle_time{120};
      std::string ssl_crt;
      std::string ssl_key;
      bool disable_tls12{true};
  };

  class HttpServer final {
    public:
      HttpServer(const Config &cfg, asio::io_context::executor_type io_context)
          : m_cfg(cfg), m_ep(asio::ip::make_address(cfg.ip), cfg.port), m_io_context(io_context) {
        initSsl();
      }

      HttpServer(const HttpServer &) = delete;
      HttpServer &operator=(const HttpServer &) = delete;
      HttpServer(HttpServer &&) = delete;
      HttpServer &operator=(HttpServer &&) = delete;

      asio::awaitable<void> start() {
        m_acceptor = std::make_unique<asio::ip::tcp::acceptor>(m_io_context);
        m_acceptor->open(m_ep.protocol());
        error_code ec;
        m_acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
        [[maybe_unused]] auto _ = m_acceptor->bind(m_ep, ec);
        if (ec) {
          SIMPLE_HTTP_ERROR_LOG("bind: {}", ec.message());
          throw std::runtime_error(ec.message());
        }
        _ = m_acceptor->listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
          SIMPLE_HTTP_ERROR_LOG("listen: {}", ec.message());
          throw std::runtime_error(ec.message());
        }
        for (;;) {
          asio::ip::tcp::socket socket(m_io_context);
          auto [ec] = co_await m_acceptor->async_accept(socket, asio::as_tuple(asio::use_awaitable));
          if (ec) {
            if (ec == asio::error::operation_aborted)
              break;
            continue;
          }
          auto endpoint = socket.remote_endpoint(ec);
          if (!ec) {
            SIMPLE_HTTP_INFO_LOG("new connection from [{}:{}]", endpoint.address().to_string(), endpoint.port());
          }
          socket.set_option(asio::socket_base::keep_alive(true));
          // socket.set_option(asio::ip::tcp::no_delay(true));
          // socket.set_option(asio::socket_base::send_buffer_size(5024 *
          // 1024));
          if (m_cfg.ssl_crt.empty()) {
            asio::co_spawn(m_io_context,
                           session(std::make_shared<asio::ip::tcp::socket>(std::move(socket)), m_io_context),
                           asio::detached);
          } else {
            auto stream = std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(std::move(socket), m_ssl_context);
            asio::co_spawn(m_io_context, startSslsession(std::move(stream), m_io_context), asio::detached);
          }
        }
      }

      void stop() {
        std::promise<void> done;
        asio::post(m_io_context, [&] {
          if (m_acceptor)
            m_acceptor->close();
          done.set_value();
        });
        done.get_future().wait();
      }

      auto &setHttpHandler(const std::string &path,
                           std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                               std::shared_ptr<HttpResponseWriter>)> cb) {
        m_handler_functions->setHttpHandler(path, std::move(cb));
        return *this;
      }

      auto &setHttpRegexHandler(const std::string &regex,
                                std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                                    std::shared_ptr<HttpResponseWriter>)> cb) {
        m_handler_functions->setHttpRegexHandler(regex, std::move(cb));
        return *this;
      }

      auto &setUnhandled(std::function<asio::awaitable<void>(http::request<http::string_body>,
                                                             std::shared_ptr<HttpResponseWriter>)> cb) {
        m_handler_functions->setUnhandled(std::move(cb));
        return *this;
      }

      auto &setBefore(std::function<asio::awaitable<bool>(const http::request<http::string_body> &,
                                                          const std::shared_ptr<HttpResponseWriter> &)> cb) {
        m_handler_functions->setBefore(std::move(cb));
        return *this;
      }

    private:
      void initSsl() {
        if (m_cfg.ssl_crt.empty() || m_cfg.ssl_key.empty())
          return;

        auto file_exists = [](const std::filesystem::path &path) {
          std::error_code ec;
          bool exists = std::filesystem::exists(path, ec) && std::filesystem::is_regular_file(path, ec);

          return !ec && exists;
        };

        if (!file_exists(m_cfg.ssl_crt)) {
          SIMPLE_HTTP_ERROR_LOG("ssl_crt not exist {}", m_cfg.ssl_crt);
          throw std::runtime_error(std::format("{} not exist", m_cfg.ssl_crt));
        }
        if (!file_exists(m_cfg.ssl_key)) {
          SIMPLE_HTTP_ERROR_LOG("ssl_key not exist {}", m_cfg.ssl_key);
          throw std::runtime_error(std::format("{} not exist", m_cfg.ssl_key));
        }

        uint64_t opts =
            asio::ssl::context::default_workarounds | asio::ssl::context::no_tlsv1 | asio::ssl::context::no_tlsv1_1;
        if (m_cfg.disable_tls12) {
          opts |= asio::ssl::context::no_tlsv1_2;
        }
        m_ssl_context.set_options(opts);

        error_code ec;
        [[maybe_unused]]
        auto ret = m_ssl_context.use_certificate_chain_file(m_cfg.ssl_crt, ec);
        if (ec)
          throw std::runtime_error(ec.message());

        [[maybe_unused]] auto _ = m_ssl_context.use_private_key_file(m_cfg.ssl_key, asio::ssl::context::pem, ec);
        if (ec)
          throw std::runtime_error(ec.message());

        SSL_CTX_set_alpn_select_cb(
            m_ssl_context.native_handle(),
            [](SSL * /* ssl */,
               const unsigned char **out,
               unsigned char *outlen,
               const unsigned char *in,
               unsigned int inlen,
               void * /* arg */) {
              static const unsigned char alpn_proto_list[] = {
                  0x02, 'h', '2' // length-prefixed: "\x02h2"
              };
              if (SSL_select_next_proto(
                      (unsigned char **)out, outlen, alpn_proto_list, sizeof(alpn_proto_list), in, inlen) !=
                  OPENSSL_NPN_NEGOTIATED) {
                return SSL_TLSEXT_ERR_NOACK;
              }
              return SSL_TLSEXT_ERR_OK;
            },
            nullptr);
      }

      asio::awaitable<void> upgradeH2c(auto socket, http::request<http::string_body> req, std::string settings) {
        // nghttp --upgrade  http://127.0.0.1:6666/hello --data ./a.txt
        // curl -v --http2 http://localhost:6666/hello -d "aaaa" -k
        http::response<http::empty_body> res{http::status::switching_protocols, 11};
        res.set(http::field::connection, "Upgrade");
        res.set(http::field::upgrade, "h2c");
        co_await http::async_write(*socket, res, asio::as_tuple(asio::use_awaitable));

        auto ch = std::make_shared<Http2Channel>(co_await asio::this_coro::executor, CHANNEL_SIZE);
        auto h2p = std::make_shared<Http2Parse>(ch, nullptr, m_io_context, m_handler_functions);
        if (auto ret = h2p->init(Http2Parse::Config{
                .is_h2c_upgrade = true,
                .h2_setting = std::move(settings),
                .concurrent_streams = m_cfg.concurrent_streams,
                .window_size = m_cfg.window_size,
                .max_frame_size = m_cfg.max_frame_size,
                .method = req.method(),
            });
            ret) {
          SIMPLE_HTTP_ERROR_LOG("init error: {}", ret);
          co_return;
        }

        if (req.method() != http::verb::options) {
          callHandler(m_handler_functions,
                      m_io_context,
                      std::move(req),
                      std::make_shared<HttpResponseWriter>(h2p, 1, Version::Http2));
        }
        auto deadline = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
        co_await (toH2Parse(socket, h2p, deadline, m_cfg.max_idle_time) ||
                  toSocket(socket, ch, deadline, m_cfg.max_idle_time) || watchdog(deadline));
        shutdown(socket);
      }

      asio::awaitable<void> switchH2c(auto socket, const std::string &buffer) {
        // start proc http2
        auto ch = std::make_shared<Http2Channel>(co_await asio::this_coro::executor, CHANNEL_SIZE);
        auto h2p = std::make_shared<Http2Parse>(ch, nullptr, m_io_context, m_handler_functions);
        if (auto ret = h2p->init(Http2Parse::Config{
                .is_h2c_upgrade = false,
                .h2_setting = "",
                .concurrent_streams = m_cfg.concurrent_streams,
                .window_size = m_cfg.window_size,
                .max_frame_size = m_cfg.max_frame_size,
                .method = http::verb::get, // not use
            });
            ret) {
          SIMPLE_HTTP_ERROR_LOG("init error: {}", ret);
          co_return;
        }
        auto ret = h2p->feedRecvData(buffer.c_str(), buffer.size());
        if (ret == -1) {
          co_return;
        }
        auto deadline = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
        co_await (toH2Parse(socket, h2p, deadline, m_cfg.max_idle_time) ||
                  toSocket(socket, ch, deadline, m_cfg.max_idle_time) || watchdog(deadline));
        shutdown(socket);
      }

      asio::awaitable<void> switchHttp1(auto socket,
                                        std::shared_ptr<Http1Channel> http1_ch,
                                        std::shared_ptr<Http2Parse> h2p,
                                        Version version) {
        auto recv_request = [this](auto socket, auto h2p, auto deadline, auto max_idle_time) -> asio::awaitable<void> {
          for (;;) {
            *deadline = std::chrono::steady_clock::now() + max_idle_time;
            beast::flat_buffer buffer;
            http::request<http::string_body> req;
            auto [ec, count] = co_await http::async_read(*socket, buffer, req, asio::as_tuple(asio::use_awaitable));
            if (ec) {
              co_return;
            }
            auto version = (req.version() == 11 ? Version::Http11 : Version::Http1);
            callHandler(m_handler_functions,
                        m_io_context,
                        std::move(req),
                        std::make_shared<HttpResponseWriter>(h2p, 0, version));
          }
        };

        auto send_response = [](auto socket, auto http1_ch, Version version, auto deadline, auto max_idle_time)
            -> asio::awaitable<void> {
          while (true) {
            *deadline = std::chrono::steady_clock::now() + max_idle_time;

            auto [ec, h1_rsp] = co_await http1_ch->async_receive(asio::as_tuple(asio::use_awaitable));

            if (ec) {
              break;
            }
            if (std::holds_alternative<std::string>(h1_rsp)) {
              auto &body = std::get<std::string>(h1_rsp);
              if (auto [ec, count] = co_await asio::async_write(
                      *socket, asio::buffer(body.data(), body.size()), asio::as_tuple(asio::use_awaitable));
                  ec) {
                break;
              }
            } else if (std::holds_alternative<std::shared_ptr<http::response<http::string_body>>>(h1_rsp)) {
              auto &body = std::get<std::shared_ptr<http::response<http::string_body>>>(h1_rsp);
              if (auto [ec, count] = co_await http::async_write(*socket, *body, asio::as_tuple(asio::use_awaitable));
                  ec) {
                break;
              }
            } else {
              // forceClose
              break;
            }
            if (version == Version::Http1)
              break;
          }
          co_return;
        };

        auto deadline = std::make_shared<std::chrono::steady_clock::time_point>(std::chrono::steady_clock::now());
        co_await (recv_request(socket, h2p, deadline, m_cfg.max_idle_time) ||
                  send_response(socket, http1_ch, version, deadline, m_cfg.max_idle_time) || watchdog(deadline));
        shutdown(socket);
        co_return;
      }

      asio::awaitable<void> session(auto socket, asio::io_context::executor_type io_context) {
        auto http1_ch = std::make_shared<Http1Channel>(io_context, CHANNEL_SIZE);
        auto h2p = std::make_shared<Http2Parse>(nullptr, http1_ch, m_io_context, m_handler_functions);
        beast::flat_buffer buffer;
        http::parser<true, http::string_body> parser;
        auto [ec, bytes] =
            co_await http::async_read_header(*socket, buffer, parser, asio::as_tuple(asio::use_awaitable));
        if (ec == http::error::end_of_stream) {
          co_return;
        }
        if (ec == http::error::bad_version) {
          auto req_str = beast::buffers_to_string(buffer.data());
          if (isHttp2(req_str)) {
            co_await switchH2c(std::move(socket), req_str);
          } else {
            SIMPLE_HTTP_ERROR_LOG("not http2 request");
          }
          co_return;
        }
        if (ec) {
          co_return;
        }
        auto &headers = parser.get();

        std::string h2_setting;
        if (headers.find(http::field::upgrade) != headers.end() && headers[http::field::upgrade] == "h2c") {
          constexpr auto http2_header = "HTTP2-Settings";
          if (headers.contains(http2_header)) {
            h2_setting = headers.at(http2_header);
          }
        }

        std::tie(ec, bytes) = co_await http::async_read(*socket, buffer, parser, asio::as_tuple(asio::use_awaitable));
        if (ec) {
          SIMPLE_HTTP_ERROR_LOG("body read error: {}", ec.message());
          co_return;
        }

        http::request<http::string_body> full_req = parser.get();

        if (!h2_setting.empty()) {
          co_await upgradeH2c(std::move(socket), std::move(full_req), std::move(h2_setting));
          co_return;
        }

        // this is http1 or 1.1
        auto version = (full_req.version() == 11 ? Version::Http11 : Version::Http1);
        callHandler(m_handler_functions,
                    m_io_context,
                    std::move(full_req),
                    std::make_shared<HttpResponseWriter>(h2p, 0, version));
        co_await switchHttp1(std::move(socket), http1_ch, h2p, version);

        co_return;
      }

      // https server
      asio::awaitable<void> startSslsession(std::shared_ptr<asio::ssl::stream<asio::ip::tcp::socket>> socket,
                                            asio::io_context::executor_type io_context) {
        if (auto [ec] = co_await socket->async_handshake(boost::asio::ssl::stream_base::server,
                                                         asio::as_tuple(asio::use_awaitable));
            ec) {
          SIMPLE_HTTP_DEBUG_LOG("async_handshake: {}", ec.message());
          co_return;
        }
        asio::co_spawn(io_context, session(std::move(socket), io_context), asio::detached);
      }

      Config m_cfg;
      asio::ip::tcp::endpoint m_ep;
      asio::io_context::executor_type m_io_context;
      asio::ssl::context m_ssl_context{asio::ssl::context::tlsv13_server};
      std::unique_ptr<asio::ip::tcp::acceptor> m_acceptor;
      std::shared_ptr<HandlerFunctions> m_handler_functions = std::make_shared<HandlerFunctions>();
  };
}
