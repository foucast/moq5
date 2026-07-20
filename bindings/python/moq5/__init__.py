"""
moq5: Python bindings for Red5's MoQ5 (sans-I/O MOQT protocol library).

A wrapper over MoQ5's Core Session API (session.h) and its publisher/
subscriber facades (publisher.h/subscriber.h). No relay or orchestration
logic lives here, and no transport integration (e.g. an asyncio-based
QUIC stack) -- this module only wraps the protocol-engine C API itself.

MoQ5 is sans-I/O: it owns no sockets, no threads, no event loop. The
caller (this module's user) is responsible for moving bytes between
sessions over whatever transport it chooses -- see Session.feed_control/
feed_datagram (inbound) and Session.poll_actions (outbound).
"""

from ._moq5_cffi import ffi, lib

# -- Re-exported constants ------------------------------------------

MOQ_OK = lib.MOQ_OK
MOQ_DONE = lib.MOQ_DONE
MOQ_ERR_WOULD_BLOCK = lib.MOQ_ERR_WOULD_BLOCK

MOQ_PERSPECTIVE_CLIENT = lib.MOQ_PERSPECTIVE_CLIENT
MOQ_PERSPECTIVE_SERVER = lib.MOQ_PERSPECTIVE_SERVER

MOQ_SESS_IDLE = lib.MOQ_SESS_IDLE
MOQ_SESS_SETUP_SENT = lib.MOQ_SESS_SETUP_SENT
MOQ_SESS_ESTABLISHED = lib.MOQ_SESS_ESTABLISHED
MOQ_SESS_DRAINING = lib.MOQ_SESS_DRAINING
MOQ_SESS_CLOSED = lib.MOQ_SESS_CLOSED

MOQ_PUB_REJECT_ALL = lib.MOQ_PUB_REJECT_ALL
MOQ_PUB_ACCEPT_ALL = lib.MOQ_PUB_ACCEPT_ALL
MOQ_PUB_CALLBACK = lib.MOQ_PUB_CALLBACK


class MoqError(Exception):
    """Raised when a MoQ5 call returns a negative moq_result_t.

    MOQ_DONE (a positive sentinel, not an error -- see types.h's own
    "if (rc < 0)" convention comment) is handled by callers directly,
    not raised as an exception (see Subscriber.poll_object).
    """

    def __init__(self, rc, call_name=""):
        self.rc = rc
        try:
            msg = ffi.string(lib.moq_strerror(rc)).decode("utf-8", "replace")
        except Exception:
            msg = "<moq_strerror unavailable>"
        prefix = f"{call_name}: " if call_name else ""
        super().__init__(f"{prefix}{msg} (rc={rc})")


def _check(rc, call_name=""):
    if rc < 0:
        raise MoqError(rc, call_name)
    return rc


def default_alloc():
    """Returns MoQ5's built-in libc-backed allocator (moq_alloc_default()).

    Used throughout instead of a Python-backed moq_alloc_t: the
    allocator's fields are C function pointers, and MoQ5 may call
    alloc/realloc/free frequently and mid-C-call -- a Python-backed
    version would need every call wrapped in ffi.callback() (real
    per-call overhead) and careful lifetime bookkeeping to keep
    Python-owned memory alive exactly as long as C holds a pointer to
    it, for no benefit this binding currently needs. Revisit only if a
    real instrumentation need (e.g. allocation counting for benchmarking)
    arises later.
    """
    return lib.moq_alloc_default()


def _make_bytes(data: bytes):
    """Build a moq_bytes_t borrowing from a copy of `data`.

    Returns (moq_bytes_t*, keepalive_cdata). The caller MUST keep
    keepalive_cdata alive for as long as the moq_bytes_t (or anything
    holding a copy of its .data pointer) is in use -- moq_bytes_t only
    borrows; it does not own or copy `data` itself.
    """
    cdata = ffi.new("uint8_t[]", data)
    view = ffi.new("moq_bytes_t *")
    view.data = cdata
    view.len = len(data)
    return view, cdata


def _make_namespace(parts):
    """Build a moq_namespace_t from a list of bytes parts.

    Returns (moq_namespace_t*, keepalive_list). Same borrowing caveat
    as _make_bytes -- keepalive_list must outlive the namespace's use.
    """
    keepalive = []
    flat = []
    for part in parts:
        view, cdata = _make_bytes(part)
        flat.append(view[0])
        keepalive.append(cdata)
    arr = ffi.new("moq_bytes_t[]", flat)
    ns = ffi.new("moq_namespace_t *")
    ns.parts = arr
    ns.count = len(parts)
    keepalive.append(arr)
    return ns, keepalive


class Session:
    """Wraps a raw moq_session_t.

    Sans-I/O: this class owns no transport. The caller must feed
    inbound bytes (feed_control/feed_datagram) and drain outbound
    actions (poll_actions) itself, driving whatever real or simulated
    transport connects this session to its peer.
    """

    def __init__(
        self,
        perspective,
        now_us=0,
        send_request_capacity=True,
        initial_request_capacity=64,
    ):
        alloc = default_alloc()
        cfg = ffi.new("moq_session_cfg_t *")
        lib.moq_session_cfg_init_sized(
            cfg, ffi.sizeof("moq_session_cfg_t"), alloc, perspective
        )
        cfg.send_request_capacity = send_request_capacity
        cfg.initial_request_capacity = initial_request_capacity

        out = ffi.new("moq_session_t **")
        _check(lib.moq_session_create(cfg, now_us, out), "moq_session_create")
        self._ptr = out[0]
        self._destroyed = False

    @property
    def ptr(self):
        """The raw moq_session_t* -- needed by Publisher/Subscriber,
        which wrap an existing session rather than creating their own
        (matching moq_pub_create/moq_sub_create's real signatures)."""
        return self._ptr

    def start(self, now_us):
        """Begin the handshake (client role only -- see reference
        examples/picoquic/{publisher,subscriber}.c: only the CLIENT-
        perspective session calls start(); the SERVER-perspective side
        becomes active reactively upon receiving CLIENT_SETUP via
        feed_control())."""
        _check(lib.moq_session_start(self._ptr, now_us), "moq_session_start")

    @property
    def state(self):
        return lib.moq_session_state(self._ptr)

    def feed_control(self, data: bytes, now_us):
        _check(
            lib.moq_session_on_control_bytes(self._ptr, data, len(data), now_us),
            "moq_session_on_control_bytes",
        )

    def feed_datagram(self, data: bytes, now_us):
        _check(
            lib.moq_session_on_datagram(self._ptr, data, len(data), now_us),
            "moq_session_on_datagram",
        )

    def tick(self, now_us):
        """Raw session tick. NOTE: when this session is owned by a
        Publisher/Subscriber facade, call the FACADE's .tick() instead
        (which drains this session's events internally) -- do not call
        both; see the design doc's sequencing rule (raw poll_events and
        facade tick are mutually exclusive per session)."""
        _check(lib.moq_session_tick(self._ptr, now_us), "moq_session_tick")

    def poll_actions(self, cap=16):
        """Drain pending outbound actions via the shim (see _ffi_build.py
        for why this doesn't call moq_session_poll_actions directly).

        Returns a list of dicts. Kinds interpreted: send_control,
        send_datagram, close_session. Anything else is reported as
        {"kind": "other", "raw_kind": <int>} and cleaned up without
        further interpretation -- this binding's usage (datagram-mode
        object delivery under draft-16's shared control stream) does
        not expect to see SEND_DATA/OPEN_*/RESET_*/STOP_* actions; if one
        appears, surfacing it as "other" rather than silently dropping
        it makes an unexpected action visible to test/debugging code
        rather than hidden.
        """
        buf = ffi.new(f"moq_action_t[{cap}]")
        n = lib.moq5_shim_poll_actions(self._ptr, buf, cap)
        results = []
        for i in range(n):
            a = buf[i]
            if a.kind == lib.MOQ_ACTION_SEND_CONTROL:
                data = bytes(
                    ffi.buffer(a.u.send_control.data, a.u.send_control.len)
                )
                results.append({"kind": "send_control", "data": data})
            elif a.kind == lib.MOQ_ACTION_SEND_DATAGRAM:
                data = bytes(
                    ffi.buffer(a.u.send_datagram.data, a.u.send_datagram.len)
                )
                results.append({"kind": "send_datagram", "data": data})
            elif a.kind == lib.MOQ_ACTION_CLOSE_SESSION:
                results.append(
                    {"kind": "close_session", "code": int(a.u.close_session.code)}
                )
            else:
                results.append({"kind": "other", "raw_kind": int(a.kind)})
            lib.moq_action_cleanup(ffi.addressof(buf[i]))
        return results

    def destroy(self):
        if not self._destroyed:
            lib.moq_session_destroy(self._ptr)
            self._destroyed = True

    def __del__(self):
        try:
            self.destroy()
        except Exception:
            pass


class Publisher:
    """Wraps moq_publisher_t, the publisher-side facade.

    Must wrap an existing Session (moq_pub_create takes a raw
    moq_session_t* as its first argument -- the facade does not create
    its own session).
    """

    def __init__(self, session: Session, accept_mode=None):
        if accept_mode is None:
            accept_mode = MOQ_PUB_ACCEPT_ALL
        alloc = default_alloc()
        cfg = ffi.new("moq_pub_cfg_t *")
        lib.moq_pub_cfg_init_sized(cfg, ffi.sizeof("moq_pub_cfg_t"))
        cfg.accept_mode = accept_mode

        out = ffi.new("moq_publisher_t **")
        _check(
            lib.moq_pub_create(session.ptr, alloc, cfg, out), "moq_pub_create"
        )
        self._ptr = out[0]
        self._destroyed = False
        self._keepalive = []  # namespace/name buffers from add_track calls

    def tick(self, now_us):
        _check(lib.moq_pub_tick(self._ptr, now_us), "moq_pub_tick")

    def add_track(self, namespace_parts, track_name: bytes, now_us):
        cfg = ffi.new("moq_pub_track_cfg_t *")
        lib.moq_pub_track_cfg_init(cfg)
        ns, ns_keepalive = _make_namespace(namespace_parts)
        name_view, name_keepalive = _make_bytes(track_name)
        cfg.track_namespace = ns[0]
        cfg.track_name = name_view[0]
        self._keepalive.append((ns, ns_keepalive, name_view, name_keepalive))

        out = ffi.new("moq_pub_track_t **")
        _check(
            lib.moq_pub_add_track(self._ptr, cfg, now_us, out),
            "moq_pub_add_track",
        )
        return out[0]

    def has_subscriber(self, track):
        return bool(lib.moq_pub_has_subscriber(self._ptr, track))

    def write_object(
        self, track, group_id, object_id, payload: bytes, now_us, datagram=True
    ):
        """Publish one object. datagram=True (the default) forces
        datagram-mode delivery via moq_pub_object_cfg_t.datagram,
        sidestepping stream_ref bookkeeping entirely -- stream-mode
        delivery is a separate, larger integration concern this binding
        does not need to cover."""
        alloc = default_alloc()
        rcbuf_out = ffi.new("moq_rcbuf_t **")
        _check(
            lib.moq_rcbuf_create(alloc, payload, len(payload), rcbuf_out),
            "moq_rcbuf_create",
        )
        buf = rcbuf_out[0]
        try:
            obj = ffi.new("moq_pub_object_cfg_t *")
            lib.moq_pub_object_cfg_init(obj)
            obj.group_id = group_id
            obj.object_id = object_id
            obj.payload = buf
            obj.properties = ffi.NULL
            obj.datagram = datagram
            _check(
                lib.moq_pub_write_object_ex(self._ptr, track, obj, now_us),
                "moq_pub_write_object_ex",
            )
        finally:
            # write_object_ex's own reference semantics mirror the
            # reference example (examples/picoquic/publisher.c): the
            # caller decrefs its own creation reference immediately
            # after the write call.
            lib.moq_rcbuf_decref(buf)

    def destroy(self):
        if not self._destroyed:
            lib.moq_pub_destroy(self._ptr)
            self._destroyed = True

    def __del__(self):
        try:
            self.destroy()
        except Exception:
            pass


class Subscriber:
    """Wraps moq_subscriber_t, the subscriber-side facade."""

    def __init__(self, session: Session):
        alloc = default_alloc()
        cfg = ffi.new("moq_sub_cfg_t *")
        lib.moq_sub_cfg_init(cfg)

        out = ffi.new("moq_subscriber_t **")
        _check(
            lib.moq_sub_create(session.ptr, alloc, cfg, out), "moq_sub_create"
        )
        self._ptr = out[0]
        self._destroyed = False
        self._keepalive = []

    def tick(self, now_us):
        _check(lib.moq_sub_tick(self._ptr, now_us), "moq_sub_tick")

    def subscribe(self, namespace_parts, track_name: bytes, now_us):
        cfg = ffi.new("moq_sub_track_cfg_t *")
        lib.moq_sub_track_cfg_init(cfg)
        ns, ns_keepalive = _make_namespace(namespace_parts)
        name_view, name_keepalive = _make_bytes(track_name)
        cfg.track_namespace = ns[0]
        cfg.track_name = name_view[0]
        self._keepalive.append((ns, ns_keepalive, name_view, name_keepalive))

        out = ffi.new("moq_sub_track_t **")
        _check(
            lib.moq_sub_subscribe(self._ptr, cfg, now_us, out),
            "moq_sub_subscribe",
        )
        return out[0]

    def poll_object(self):
        """Poll the next received object.

        Returns a dict {"group_id", "object_id", "status", "datagram",
        "payload": bytes|None} or None if no object is currently queued
        (MOQ_DONE -- not an error, per moq_sub_poll_object's own doc
        comment: "Returns MOQ_DONE when no objects are queued.").
        """
        obj = ffi.new("moq_sub_object_t *")
        rc = lib.moq_sub_poll_object(self._ptr, obj)
        if rc == MOQ_DONE:
            return None
        _check(rc, "moq_sub_poll_object")

        payload = None
        if obj.payload != ffi.NULL:
            payload = bytes(
                ffi.buffer(
                    lib.moq_rcbuf_data(obj.payload), lib.moq_rcbuf_len(obj.payload)
                )
            )
        result = {
            "group_id": int(obj.group_id),
            "object_id": int(obj.object_id),
            "status": int(obj.status),
            "datagram": bool(obj.datagram),
            "payload": payload,
        }
        lib.moq_sub_object_cleanup(obj)
        return result

    def destroy(self):
        if not self._destroyed:
            lib.moq_sub_destroy(self._ptr)
            self._destroyed = True

    def __del__(self):
        try:
            self.destroy()
        except Exception:
            pass
