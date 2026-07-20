# moq5 Python binding

A `cffi`-based Python wrapper over MoQ5's Core Session API
(`core/include/moq/session.h`) and its publisher/subscriber facades
(`publisher.h`/`subscriber.h`). No relay logic, transport integration,
or application-level orchestration lives here -- this wraps the
protocol-engine C API itself, faithfully, for use by applications that
provide their own transport (MoQ5 is sans-I/O: it owns no sockets, no
threads, no event loop).

## Why `cffi` API mode

`session.h` defines two convenience functions
(`moq_session_poll_actions`, `moq_session_poll_events`) as `static
inline` C -- compiled into whatever includes the header, not exported
as symbols in `libmoq`. `cffi`'s ABI mode (`dlopen`/`dlsym`) cannot call
a function that was never compiled into the library as a linkable
symbol. This binding uses `cffi`'s API mode instead: a small shim
(`moq5/_ffi_build.py`'s `SHIM_SOURCE`) `#include`s the real header and
re-exports both functions, so the compiler inlines whatever the header
currently says -- no hand-copied logic, no risk of silently drifting
out of sync with a future header change (a change that breaks the
shim's assumptions becomes a build-time compile error, not a silent
runtime bug).

## Building

1. Build the core library from the repository root (if not already built):
   ```sh
   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
   cmake --build build
   ```
2. Compile the Python extension:
   ```sh
   cd bindings/python
   pip install cffi
   python3 moq5/_ffi_build.py
   ```
   This produces `moq5/_moq5_cffi*.so`. By default it looks for headers
   at `<repo_root>/core/include` and the built library at
   `<repo_root>/build/core` -- override with the `MOQ5_INCLUDE_DIR` /
   `MOQ5_LIB_DIR` environment variables for a different layout.

## Running the acceptance test

```sh
cd bindings/python
python3 tests/test_e2e_single_hop.py
```

This validates that the bindings compose correctly across a real MoQT
session lifecycle: one publisher session emits a known object, a
minimal single-hop relay (built from nothing but this binding's own
`Session`/`Publisher`/`Subscriber` classes) forwards it, and a final
subscriber session receives it. Pass/fail is exact byte equality
between what was sent and what was received. See the test file's own
docstring for the full methodology and what this test does and does
not validate.

## Scope

Covers: session lifecycle, the control/data/datagram feed-and-drain
loop, the publisher and subscriber facades, object read/write, and the
raw `moq_session_accept_subscribe` / `moq_accept_subscribe_cfg_t` /
`moq_session_send_object_datagram` calls (needed when a caller wants to
pin a specific `track_alias` at accept time, rather than accept the
facade's auto-generated one -- relevant once more than one downstream
session needs byte-identical encoded output for the same object).

Does not cover: `codec.h`/`wire.h` (draft-specific wire internals,
explicitly excluded from the Core Session API's own include chain),
the media-service tier (`endpoint.h`/`media_receiver.h`/
`media_sender.h` -- owns its own transport, a different integration
shape than this binding targets), or any transport adapter.
