"""
Build script for the moq5 Python binding's compiled cffi extension.

Uses cffi's out-of-line API mode: `ffi.set_source()` provides real C
source (compiled by a C compiler against MoQ5's actual headers) and
`ffi.cdef()` provides the interface declarations cffi exposes to Python.

Why API mode (not ABI/dlopen mode): MoQ5's session.h defines two
convenience functions (`moq_session_poll_actions`, `moq_session_poll_events`)
as `static inline` C, meaning they are compiled into whatever includes
the header rather than exported as symbols in libmoq. ABI-mode cffi
(dlopen/dlsym) cannot find or call functions that were never compiled
into the shared/static library as linkable symbols. API mode compiles a
small shim (below) that #includes the real header, so the C compiler
inlines whatever the header currently says with zero hand-copied logic
and zero drift risk on MoQ5 upgrades.

Struct declarations below deliberately use cffi's partial-struct "..."
syntax wherever we don't need to read/write every field: we name only the
fields this binding's Python code actually touches, in the same relative
order as the real header, and let "...;" (compiled against the real
header, thanks to set_source's #include) supply everything else. This
is far less transcription than a full field-for-field copy of every
struct in session.h/publisher.h/subscriber.h -- most of it we correctly
never touch -- and cffi's build step verifies these partial declarations
against the compiled header's real layout, catching a wrong field type/
order as a build-time error rather than a silent runtime memory bug.
"""

import os
from cffi import FFI

ffi = FFI()

# ---------------------------------------------------------------------
# cdef(): the interface this binding exposes to Python.
# ---------------------------------------------------------------------
ffi.cdef(
    r"""
/* -- Result codes (types.h) ----------------------------------------
 * moq_result_t is plain `int`; MOQ_OK/MOQ_DONE/MOQ_ERR_* are #define
 * constants, not a C enum. Pulling their values via "..." asks cffi to
 * read the real value from the compiled header rather than trusting a
 * hand-typed number.
 */
typedef int moq_result_t;

#define MOQ_OK ...
#define MOQ_DONE ...
#define MOQ_ERR_NOMEM ...
#define MOQ_ERR_INVAL ...
#define MOQ_ERR_PROTO ...
#define MOQ_ERR_CLOSED ...
#define MOQ_ERR_WRONG_STATE ...
#define MOQ_ERR_STALE_HANDLE ...
#define MOQ_ERR_WRONG_SESSION ...
#define MOQ_ERR_WOULD_BLOCK ...
#define MOQ_ERR_BUFFER ...
#define MOQ_ERR_REQUEST_BLOCKED ...
#define MOQ_ERR_ABI_MISMATCH ...
#define MOQ_ERR_GOAWAY ...

const char *moq_strerror(moq_result_t rc);

/* -- Byte span / namespace (types.h) --------------------------------
 * Small, simple structs we construct directly from Python (track
 * names/namespaces) -- full field declarations, not partial.
 */
typedef struct moq_bytes {
    const uint8_t *data;
    size_t         len;
} moq_bytes_t;

typedef struct moq_namespace {
    const moq_bytes_t *parts;
    size_t             count;
} moq_namespace_t;

/* -- Allocator (types.h) ---------------------------------------------
 * Opaque from Python's side: we only ever hold and pass along the
 * pointer moq_alloc_default() returns, never touch its fields.
 */
typedef struct moq_alloc moq_alloc_t;
const moq_alloc_t *moq_alloc_default(void);

/* -- Refcounted buffer (rcbuf.h) ------------------------------------ */
typedef struct moq_rcbuf moq_rcbuf_t;

moq_result_t moq_rcbuf_create(const moq_alloc_t *alloc,
                               const uint8_t *data, size_t len,
                               moq_rcbuf_t **out);
moq_rcbuf_t *moq_rcbuf_incref(moq_rcbuf_t *buf);
void moq_rcbuf_decref(moq_rcbuf_t *buf);
const uint8_t *moq_rcbuf_data(const moq_rcbuf_t *buf);
size_t moq_rcbuf_len(const moq_rcbuf_t *buf);
uint32_t moq_rcbuf_refcount(const moq_rcbuf_t *buf);

/* -- Stream identity (types.h) ---------------------------------------
 * Only declared so moq_send_data_action_t (below) can name a field of
 * this type; the accompanying acceptance test forces datagram-mode
 * object delivery and does not expect to see SEND_DATA actions, but
 * the type must still exist for moq_action_t's union to be well-formed.
 */
typedef struct moq_stream_ref { uint64_t _v; } moq_stream_ref_t;

/* -- Subscription/publication handles (types.h) -----------------------
 * Fixed-width opaque handles, passed by value. Declared early since
 * several later declarations (the SUBSCRIBE_REQUEST event variant,
 * moq_session_subscribe, moq_session_accept_subscribe,
 * moq_session_send_object_datagram) all reference moq_subscription_t.
 */
typedef struct moq_subscription { uint64_t _opaque; } moq_subscription_t;
typedef struct moq_publication  { uint64_t _opaque; } moq_publication_t;

/* -- Perspective / session state (session.h) ------------------------
 * Real C enums -- declared with their literal values exactly as the
 * header defines them (no "..." needed; enums are fully expressible in
 * plain cdef syntax and these are small/stable).
 */
typedef enum {
    MOQ_PERSPECTIVE_CLIENT = 1,
    MOQ_PERSPECTIVE_SERVER = 2,
} moq_perspective_t;

typedef enum {
    MOQ_SESS_IDLE        = 0,
    MOQ_SESS_SETUP_SENT  = 1,
    MOQ_SESS_ESTABLISHED = 3,
    MOQ_SESS_DRAINING    = 4,
    MOQ_SESS_CLOSED      = 5,
} moq_session_state_t;

/* -- Session (session.h) --------------------------------------------
 * Opaque handle. moq_session_cfg_t: only the two fields this binding
 * sets after moq_session_cfg_init_sized() (which itself takes alloc/
 * perspective as direct arguments, not fields we set by hand).
 */
typedef struct moq_session moq_session_t;

typedef struct {
    bool     send_request_capacity;
    uint64_t initial_request_capacity;
    ...;
} moq_session_cfg_t;

void moq_session_cfg_init_sized(moq_session_cfg_t *cfg,
                                 size_t cfg_size,
                                 const moq_alloc_t *alloc,
                                 moq_perspective_t perspective);

moq_result_t moq_session_create(const moq_session_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_session_t **out);
void moq_session_destroy(moq_session_t *s);
moq_result_t moq_session_start(moq_session_t *s, uint64_t now_us);
moq_session_state_t moq_session_state(const moq_session_t *s);

/* -- I/O feed/drain loop (session.h) ---------------------------------
 * on_control_bytes/on_data_bytes/on_datagram/tick/action_cleanup/
 * event_cleanup are real MOQ_API exported symbols -- bound directly.
 * poll_actions_ex/poll_events_ex (the real, exported "_ex" functions)
 * are ALSO bound directly, since the shim's poll_actions/poll_events
 * wrappers below are the ones actually used, but exposing the real _ex
 * symbols too costs nothing and matches the documented function surface.
 */
moq_result_t moq_session_on_control_bytes(moq_session_t *s,
                                           const uint8_t *data, size_t len,
                                           uint64_t now_us);
moq_result_t moq_session_on_data_bytes(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       const uint8_t *data, size_t len,
                                       bool fin, uint64_t now_us);
moq_result_t moq_session_on_datagram(moq_session_t *s,
                                      const uint8_t *data, size_t len,
                                      uint64_t now_us);
moq_result_t moq_session_tick(moq_session_t *s, uint64_t now_us);

moq_result_t moq_session_poll_actions_ex(moq_session_t *s, void *out,
                                          size_t cap, size_t element_size,
                                          size_t *out_count);
moq_result_t moq_session_poll_events_ex(moq_session_t *s, void *out,
                                         size_t cap, size_t element_size,
                                         size_t *out_count);

/* -- Actions (outbound I/O instructions; session.h) ------------------
 * moq_action_kind_t is a plain uint32_t with #define constants (never
 * renumbered, per the header's own comment) -- pulled via "..." rather
 * than hand-typed. Variant structs relevant to datagram-mode object
 * delivery and control-channel bytes are declared in full; the union's
 * "...;" tail covers every other variant not interpreted here (RESET_DATA,
 * STOP_DATA, OPEN_BIDI_STREAM, etc.) -- draft-16's shared bidi control
 * stream means OPEN_UNI_CONTROL/SEND_UNI_CONTROL are not expected to
 * appear at all.
 */
typedef uint32_t moq_action_kind_t;

#define MOQ_ACTION_SEND_CONTROL ...
#define MOQ_ACTION_CLOSE_SESSION ...
#define MOQ_ACTION_SEND_DATA ...
#define MOQ_ACTION_SEND_DATAGRAM ...

typedef struct {
    const uint8_t *data;
    size_t         len;
} moq_send_control_action_t;

typedef struct {
    uint64_t    code;
    moq_bytes_t reason;
} moq_close_session_action_t;

typedef struct {
    moq_stream_ref_t stream_ref;
    uint8_t          header[32];
    uint8_t          header_len;
    moq_rcbuf_t     *payload;
    bool             fin;
} moq_send_data_action_t;

typedef struct {
    const uint8_t *data;
    size_t         len;
} moq_send_datagram_action_t;

union moq_action_detail {
    moq_send_control_action_t  send_control;
    moq_close_session_action_t close_session;
    moq_send_data_action_t     send_data;
    moq_send_datagram_action_t send_datagram;
    ...;
};

typedef struct {
    moq_action_kind_t        kind;
    union moq_action_detail  u;
    ...;
} moq_action_t;

void moq_action_cleanup(moq_action_t *action);

/* -- Events (session.h) ----------------------------------------------
 * Kept minimal: only the one variant actually read is declared
 * (SUBSCRIBE_REQUEST, needed by the raw accept-with-pinned-alias path
 * -- see the "Raw session calls" section below). This event is only
 * ever polled BEFORE a facade takes ownership of a session's tick():
 * once a facade owns tick() (moq_pub_tick/moq_sub_tick drain and
 * dispatch events internally), calling raw poll_events on that same
 * session would double-consume against the facade -- MoQ5's own
 * sequencing rule. Other event-kind variants are not declared; add
 * them here if and when something needs to read one.
 */
typedef uint32_t moq_event_kind_t;

#define MOQ_EVENT_SUBSCRIBE_REQUEST ...

typedef struct {
    moq_subscription_t sub;
    moq_bytes_t         track_name;
    ...;
} moq_subscribe_request_event_t;

union moq_event_detail {
    moq_subscribe_request_event_t subscribe_request;
    ...;
};

typedef struct {
    moq_event_kind_t        kind;
    union moq_event_detail  u;
    ...;
} moq_event_t;

void moq_event_cleanup(moq_event_t *event);

/* -- Request error codes (session.h) --------------------------------- */
typedef uint32_t moq_request_error_t;

/* -- Object status (session.h) --------------------------------------- */
typedef uint8_t moq_object_status_t;

/* -- Subscribe filter (session.h) -------------------------------------
 * Left at library defaults via track_cfg_init(); no named constants
 * needed since this binding never sets .filter explicitly.
 */
typedef uint32_t moq_subscribe_filter_t;

/* -- Raw subscribe (session.h) ---------------------------------------
 * moq_session_subscribe is the raw (non-facade) counterpart to
 * moq_sub_subscribe. Needed so a peer session can issue a real
 * SUBSCRIBE without going through the Subscriber facade at all --
 * keeping the whole accept/pin/encode path at the raw session level,
 * for symmetry and because the encode call under test
 * (moq_session_send_object_datagram) is itself raw, not facade.
 */
typedef struct {
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    ...;
} moq_subscribe_cfg_t;

void moq_subscribe_cfg_init(moq_subscribe_cfg_t *cfg);

moq_result_t moq_session_subscribe(moq_session_t *s,
                                    const moq_subscribe_cfg_t *cfg,
                                    uint64_t now_us,
                                    moq_subscription_t *out_handle);

/* ==================================================================
 * Publisher facade (publisher.h)
 * ================================================================== */
typedef struct moq_publisher moq_publisher_t;
typedef struct moq_pub_track moq_pub_track_t;

typedef enum {
    MOQ_PUB_REJECT_ALL = 0,
    MOQ_PUB_ACCEPT_ALL = 1,
    MOQ_PUB_CALLBACK   = 2,
} moq_pub_accept_mode_t;

/* Callbacks intentionally omitted: the accompanying acceptance test
 * drives everything by polling (moq_session_state, moq_pub_has_subscriber,
 * moq_sub_poll_object) rather than registering callback function
 * pointers, matching what the reference examples (examples/picoquic/
 * publisher.c, subscriber.c) do for the same gating checks -- the
 * callbacks there are for logging only, not required for correctness.
 */
typedef struct {
    moq_pub_accept_mode_t accept_mode;
    ...;
} moq_pub_cfg_t;

void moq_pub_cfg_init_sized(moq_pub_cfg_t *cfg, size_t cfg_size);

moq_result_t moq_pub_create(moq_session_t *session,
                             const moq_alloc_t *alloc,
                             const moq_pub_cfg_t *cfg,
                             moq_publisher_t **out);
void moq_pub_destroy(moq_publisher_t *pub);
moq_result_t moq_pub_tick(moq_publisher_t *pub, uint64_t now_us);

typedef struct {
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    ...;
} moq_pub_track_cfg_t;

void moq_pub_track_cfg_init(moq_pub_track_cfg_t *cfg);

moq_result_t moq_pub_add_track(moq_publisher_t *pub,
                                const moq_pub_track_cfg_t *cfg,
                                uint64_t now_us,
                                moq_pub_track_t **out);

bool moq_pub_has_subscriber(moq_publisher_t *pub, moq_pub_track_t *track);

typedef struct {
    uint64_t             group_id;
    uint64_t             object_id;
    moq_rcbuf_t         *payload;
    moq_rcbuf_t         *properties;
    bool                 datagram;
    ...;
} moq_pub_object_cfg_t;

void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg);

moq_result_t moq_pub_write_object_ex(moq_publisher_t *pub,
                                      moq_pub_track_t *track,
                                      const moq_pub_object_cfg_t *obj,
                                      uint64_t now_us);

/* ==================================================================
 * Subscriber facade (subscriber.h)
 * ================================================================== */
typedef struct moq_subscriber moq_subscriber_t;
typedef struct moq_sub_track  moq_sub_track_t;

/* No named fields at all: this binding never sets anything on
 * moq_sub_cfg_t after init (no callbacks registered -- see the
 * publisher-side note above for why). Still needs a correctly-sized
 * cffi type to allocate/pass a pointer to moq_sub_create(); the bare
 * "...;" partial-struct form supplies that from the compiled header.
 */
typedef struct {
    ...;
} moq_sub_cfg_t;

void moq_sub_cfg_init(moq_sub_cfg_t *cfg);

moq_result_t moq_sub_create(moq_session_t *session,
                             const moq_alloc_t *alloc,
                             const moq_sub_cfg_t *cfg,
                             moq_subscriber_t **out);
void moq_sub_destroy(moq_subscriber_t *sub);
moq_result_t moq_sub_tick(moq_subscriber_t *sub, uint64_t now_us);

typedef struct {
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    ...;
} moq_sub_track_cfg_t;

void moq_sub_track_cfg_init(moq_sub_track_cfg_t *cfg);

moq_result_t moq_sub_subscribe(moq_subscriber_t *sub,
                                const moq_sub_track_cfg_t *cfg,
                                uint64_t now_us,
                                moq_sub_track_t **out_track);

typedef struct {
    uint64_t             group_id;
    uint64_t             object_id;
    moq_object_status_t  status;
    bool                 datagram;
    moq_rcbuf_t         *payload;
    moq_rcbuf_t         *properties;
    ...;
} moq_sub_object_t;

moq_result_t moq_sub_poll_object(moq_subscriber_t *sub, moq_sub_object_t *out);
void moq_sub_object_cleanup(moq_sub_object_t *obj);

/* ==================================================================
 * Raw session calls for caller-controlled subscribe acceptance
 * (session.h). track_alias is the one session-scoped field in an
 * OBJECT_DATAGRAM (per draft-16), so pinning it identically across
 * sessions via moq_accept_subscribe_cfg_t is what makes a single
 * encoded buffer reusable across multiple downstream sessions -- the
 * facade's own accept path (moq_pub_add_track's accept-decision
 * callback) does not expose a way to set it at all.
 * ================================================================== */
typedef struct {
    bool     has_track_alias;
    uint64_t track_alias;
    ...;
} moq_accept_subscribe_cfg_t;

void moq_accept_subscribe_cfg_init(moq_accept_subscribe_cfg_t *cfg);

moq_result_t moq_session_accept_subscribe(moq_session_t *s,
                                           moq_subscription_t sub,
                                           const moq_accept_subscribe_cfg_t *cfg,
                                           uint64_t now_us);

moq_result_t moq_session_send_object_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us);

/* -- Shim re-exports (see SHIM_SOURCE below) -------------------------- */
size_t moq5_shim_poll_actions(moq_session_t *s, moq_action_t *out, size_t cap);
size_t moq5_shim_poll_events(moq_session_t *s, moq_event_t *out, size_t cap);
"""
)

# ---------------------------------------------------------------------
# set_source(): real C glue, compiled against the real MoQ5 headers.
# ---------------------------------------------------------------------
#
# The #include here is what makes the two static inline functions
# (moq_session_poll_actions/moq_session_poll_events) available to be
# called by our shim wrappers below -- since this file is compiled as
# real C, the compiler inlines whatever the current header says, with
# no hand-copied logic and no drift risk on MoQ5 upgrades. If a future
# MoQ5 release changes these two functions' signatures incompatibly,
# THIS FILE WILL FAIL TO COMPILE (a build-time error), rather than
# silently misbehaving -- that is the whole point of shimming instead
# of reimplementing.
SHIM_SOURCE = r"""
/* moq.h's umbrella only pulls in export.h/version.h/types.h/rcbuf.h/
 * session.h -- it deliberately excludes the facade headers (they are
 * separate, optional layers). publisher.h/subscriber.h are included
 * explicitly here so the facade types/functions this binding uses
 * (moq_pub_track_cfg_t, moq_sub_cfg_t, moq_pub_accept_mode_t, etc.)
 * are actually visible to the compiler. */
#include "moq/moq.h"
#include "moq/publisher.h"
#include "moq/subscriber.h"

size_t moq5_shim_poll_actions(moq_session_t *s, moq_action_t *out, size_t cap) {
    return moq_session_poll_actions(s, out, cap);
}

size_t moq5_shim_poll_events(moq_session_t *s, moq_event_t *out, size_t cap) {
    return moq_session_poll_events(s, out, cap);
}
"""

# Paths are computed relative to this file's own location, assuming the
# conventional layout: this file lives at <repo_root>/bindings/python/moq5/,
# and the library is built the standard way (`cmake -S . -B build` from
# <repo_root>). Override via MOQ5_INCLUDE_DIR/MOQ5_LIB_DIR for a different
# checkout/build layout or a CI environment.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", ".."))

MOQ5_INCLUDE_DIR = os.environ.get(
    "MOQ5_INCLUDE_DIR", os.path.join(_REPO_ROOT, "core", "include")
)
MOQ5_LIB_DIR = os.environ.get(
    "MOQ5_LIB_DIR", os.path.join(_REPO_ROOT, "build", "core")
)

ffi.set_source(
    "moq5._moq5_cffi",
    SHIM_SOURCE,
    include_dirs=[MOQ5_INCLUDE_DIR],
    library_dirs=[MOQ5_LIB_DIR],
    libraries=["moq-core"],
)

if __name__ == "__main__":
    ffi.compile(verbose=True)
