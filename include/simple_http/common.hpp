#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <vector>

#include <boost/asio.hpp>

namespace simple_http {
  namespace asio = boost::asio;

  enum class Version : uint8_t {
    Http1 = 0,
    Http11 = 1,
    Http2 = 2,
  };

  class IoCtxPool final {
    public:
      IoCtxPool(std::size_t pool_size) : m_next_io_context(0), m_pool_size(pool_size) {
        if (pool_size == 0)
          throw std::runtime_error("ContextPool size is 0");
        for (std::size_t i = 0; i < pool_size; ++i) {
          create();
        }
      }

      void start() {
        for (auto &context : m_io_contexts)
          m_threads.emplace_back([&] { context->run(); });
      }

      void stop() {
        for (auto &context_ptr : m_io_contexts)
          context_ptr->stop();
        for (auto &thread : m_threads) {
          if (thread.joinable())
            thread.join();
        }
      }

      auto &getIoContext() {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return *m_io_contexts[index % m_pool_size];
      }

      auto &getIoContextPtr() {
        size_t index = m_next_io_context.fetch_add(1, std::memory_order_relaxed);
        return m_io_contexts[index % m_pool_size];
      }

      auto &getMainContext() { return m_io_contexts.back(); }

      void createMainContext() { create(); }

    private:
      void create() {
        auto io_context_ptr = std::make_shared<asio::io_context>();
        m_io_contexts.emplace_back(io_context_ptr);
        m_work.emplace_back(asio::require(io_context_ptr->get_executor(), asio::execution::outstanding_work.tracked));
      }

      std::vector<std::shared_ptr<asio::io_context>> m_io_contexts;
      std::shared_ptr<asio::io_context> m_main_ioctx;
      std::list<asio::any_io_executor> m_work{};
      std::atomic_uint64_t m_next_io_context;
      std::vector<std::thread> m_threads;
      uint64_t m_pool_size;
  };
}
