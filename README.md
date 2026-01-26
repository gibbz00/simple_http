# simple_http

## Notable differences to upstream

### Features

- Promotes the client to non-experimental status, effectively removing `_EXPERIMENT_HTTP_CLIENT_`.

#### Tooling

- Uses `xmake` as its build system and package manager.
- Uses `clang-format` for consistent formatting.

#### Structure

- Client and server are split into into separate headers. (Not a single header library.)

## Require

* C++20
* nghttp2
* boost
