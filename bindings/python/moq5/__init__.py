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


def make_shared_payload(data: bytes):
    """Build one moq_rcbuf_t from `data`, for reuse across multiple
    RawAcceptSession.send_object_datagram_shared() calls (e.g. fanning
    the same object out to N downstream sessions) instead of each
    session independently creating an identical buffer from the same
    bytes.

    Returns an opaque handle the caller owns; call
    release_shared_payload() on it exactly once when done reusing it,
    regardless of how many sessions it was sent to in between (each
    send_object_datagram_shared() call increfs/decrefs its own use
    internally -- this call's reference is separate and always needs
    releasing).

    Only safe to share across sessions that all live in one shard
    (thread/event loop) -- moq_rcbuf_t's own refcounting is documented
    as non-atomic. A caller spread across multiple threads needs
    moq_rcbuf_clone() per destination shard instead, which this helper
    does not provide.
    """
    alloc = default_alloc()
    rcbuf_out = ffi.new("moq_rcbuf_t **")
    _check(
        lib.moq_rcbuf_create(alloc, data, len(data), rcbuf_out),
        "moq_rcbuf_create",
    )
    return rcbuf_out[0]


def release_shared_payload(shared_payload):
    """Release the caller's own reference to a handle obtained from
    make_shared_payload(), once done reusing it across sends."""
    lib.moq_rcbuf_decref(shared_payload)


def shared_payload_refcount(shared_payload) -> int:
    """Current refcount of a handle from make_shared_payload(). Mainly
    useful for tests/diagnostics confirming incref/decref pairs balance
    out correctly rather than leaking or double-freeing."""
    return int(lib.moq_rcbuf_refcount(shared_payload))


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

    def feed_data(self, stream_id: int, data: bytes, fin: bool, now_us):
        """Feed bytes received on a data (object-delivery) stream.

        stream_ref is adapter-assigned and opaque to MoQ5 -- we use
        the QUIC stream_id directly as the _v value, since it uniquely
        identifies the stream and stays consistent across calls.
        MoQ5 only uses stream_ref as a lookup key; it never interprets
        the value itself."""
        stream_ref = ffi.new("moq_stream_ref_t *")
        stream_ref._v = stream_id
        _check(
            lib.moq_session_on_data_bytes(
                self._ptr, stream_ref[0], data, len(data), fin, now_us
            ),
            "moq_session_on_data_bytes",
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
        send_datagram, send_data (stream-mode object/subgroup-header
        delivery), close_session. Anything else is reported as
        {"kind": "other", "raw_kind": <int>} and cleaned up without
        further interpretation.
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
            elif a.kind == lib.MOQ_ACTION_SEND_DATA:
                header_bytes = bytes(
                    ffi.buffer(a.u.send_data.header, a.u.send_data.header_len)
                )
                payload_bytes = b""
                if a.u.send_data.payload != ffi.NULL:
                    payload_bytes = bytes(
                        ffi.buffer(
                            lib.moq_rcbuf_data(a.u.send_data.payload),
                            lib.moq_rcbuf_len(a.u.send_data.payload),
                        )
                    )
                results.append(
                    {
                        "kind": "send_data",
                        # header + payload concatenated: the full wire
                        # bytes for this stream chunk (Subgroup Header
                        # framing, or a continuation of one, followed by
                        # whatever object bytes belong with it).
                        "data": header_bytes + payload_bytes,
                        "stream_ref": int(a.u.send_data.stream_ref._v),
                        "fin": bool(a.u.send_data.fin),
                    }
                )
            elif a.kind == lib.MOQ_ACTION_CLOSE_SESSION:
                reason_bytes = (
                    bytes(ffi.buffer(a.u.close_session.reason.data, a.u.close_session.reason.len))
                    if a.u.close_session.reason.len > 0
                    else b""
                )
                results.append(
                    {
                        "kind": "close_session",
                        "code": int(a.u.close_session.code),
                        "reason": reason_bytes,
                    }
                )
            else:
                results.append({"kind": "other", "raw_kind": int(a.kind)})
            lib.moq_action_cleanup(ffi.addressof(buf[i]))
        return results

    def raw_subscribe(self, namespace_parts, track_name: bytes, now_us):
        """Issue a raw (non-facade) SUBSCRIBE -- the counterpart to
        RawAcceptSession's accept-with-pinned-alias path on the peer
        side. Returns the moq_subscription_t handle (unused by the
        caller in the common case; moq_session_subscribe requires an
        out-parameter regardless).

        This exists alongside the Subscriber facade's own .subscribe()
        because RawAcceptSession's accept counterpart is deliberately
        raw-only throughout (see RawAcceptSession's docstring for why
        the facade cannot be used on that side) -- keeping both ends of
        that exchange at the same (raw) level, rather than mixing a
        facade-driven peer against a raw-only acceptor.
        """
        cfg = ffi.new("moq_subscribe_cfg_t *")
        lib.moq_subscribe_cfg_init(cfg)
        ns, ns_keepalive = _make_namespace(namespace_parts)
        name_view, name_keepalive = _make_bytes(track_name)
        cfg.track_namespace = ns[0]
        cfg.track_name = name_view[0]
        if not hasattr(self, "_raw_keepalive"):
            self._raw_keepalive = []
        self._raw_keepalive.append((ns, ns_keepalive, name_view, name_keepalive))

        out = ffi.new("moq_subscription_t *")
        _check(
            lib.moq_session_subscribe(self._ptr, cfg, now_us, out),
            "moq_session_subscribe",
        )
        return out[0]

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


class SubscribeAcceptState:
    """States for RawAcceptSession's one-way accept sequencing gate."""

    PENDING_ACCEPT = "PENDING_ACCEPT"
    ACCEPTED = "ACCEPTED"


class RawAcceptSession:
    """A raw session that accepts exactly one incoming SUBSCRIBE with a
    caller-pinned track_alias, entirely at the raw session level -- no
    Publisher facade involved anywhere in this class.

    Why raw-only rather than raw-accept-then-facade: MoQ5's Publisher
    facade discovers subscribers by polling SUBSCRIBE_REQUEST events
    itself, inside its own tick() (moq_pub_tick's own doc comment:
    "dispatches subscribe requests"). If this class's
    poll_for_subscribe_request() drains that event first -- which it
    must, to read the pending subscription handle before deciding how
    to accept it -- there is nothing left in the queue for a facade
    created afterward to discover. Checked directly against the header:
    publisher.h has no function anywhere that takes a moq_subscription_t
    or a track_alias parameter, confirming there is no bridge from a
    raw-accepted subscription into the facade's own track/subscriber
    bookkeeping. Since moq_session_send_object_datagram (the call this
    exists to drive) is itself a raw call, not a facade one, staying
    raw-only throughout avoids needing that bridge at all: no
    Publisher, no moq_pub_add_track, no has_subscriber -- just the
    session, the accepted subscription handle, and direct calls to the
    raw encode function.

    States: PENDING_ACCEPT -> ACCEPTED, one-way. accept() is illegal
    before a SUBSCRIBE_REQUEST has actually been found, and both
    poll_for_subscribe_request() and accept() are illegal once already
    ACCEPTED -- a given instance manages exactly one accepted
    subscription for its whole lifetime. This is enforced by raising,
    not left to caller discipline, matching the same reasoning as the
    sequencing guarantee this class exists to implement in the first
    place: a hazard worth preventing mechanically is worth preventing
    mechanically in its own guard code too.
    """

    def __init__(self, perspective, now_us=0):
        self.session = Session(perspective, now_us)
        self.state = SubscribeAcceptState.PENDING_ACCEPT
        self.sub_handle = None
        self.sub_track_name = None

    def tick(self, now_us):
        self.session.tick(now_us)

    def feed_control(self, data: bytes, now_us):
        self.session.feed_control(data, now_us)

    def feed_data(self, stream_id: int, data: bytes, fin: bool, now_us):
        self.session.feed_data(stream_id, data, fin, now_us)

    def feed_datagram(self, data: bytes, now_us):
        self.session.feed_datagram(data, now_us)

    def poll_actions(self, cap=16):
        return self.session.poll_actions(cap)

    def poll_for_subscribe_request(self, cap=8):
        """Poll raw events looking for MOQ_EVENT_SUBSCRIBE_REQUEST.
        Only legal in PENDING_ACCEPT. Returns True once a request has
        been found (remembering its handle/track_name for accept() to
        use), False otherwise -- safe to call repeatedly across ticks
        until it returns True. Any other event kind encountered along
        the way is drained and cleaned up without interpretation; this
        class only cares about the one event type it exists to find.

        IMPORTANT: the polled event's .u.subscribe_request.sub field
        must be COPIED out explicitly here, not simply assigned. cffi's
        nested struct/union field access (event -> union -> nested
        struct -> field) returns a view into the enclosing
        moq_event_t[cap] array's own backing memory, not an
        independent copy -- even though moq_subscription_t is a small
        value type ({ uint64_t _opaque; }), not a pointer. Once `buf`
        below goes out of scope (or once a later ffi.new() call, e.g.
        inside poll_actions(), reuses that same memory), a bare
        assignment of self.sub_handle = ev.u.subscribe_request.sub
        would silently start reflecting whatever now occupies that
        memory: symptom is the handle's _opaque value reading back as
        0 after any intervening poll_actions() call, and
        moq_session_send_object_datagram failing with
        MOQ_ERR_STALE_HANDLE despite nothing re-assigning the Python
        attribute. The fix below uses cffi's copy-construct idiom --
        ffi.new("moq_subscription_t *", <existing cdata>) -- which
        allocates fresh, Python-owned memory and performs a memberwise
        copy into it, independent of the event array's lifetime. This
        stays correct automatically if this struct's fields ever
        change, with no per-field maintenance.
        """
        if self.state != SubscribeAcceptState.PENDING_ACCEPT:
            raise RuntimeError(
                f"poll_for_subscribe_request() is only legal in "
                f"PENDING_ACCEPT (current state: {self.state})"
            )
        if self.sub_handle is not None:
            return True

        buf = ffi.new(f"moq_event_t[{cap}]")
        n = lib.moq5_shim_poll_events(self.session.ptr, buf, cap)
        found = False
        for i in range(n):
            ev = buf[i]
            if ev.kind == lib.MOQ_EVENT_SUBSCRIBE_REQUEST and self.sub_handle is None:
                copied_handle = ffi.new("moq_subscription_t *", ev.u.subscribe_request.sub)
                self.sub_handle = copied_handle[0]
                self.sub_track_name = bytes(
                    ffi.buffer(
                        ev.u.subscribe_request.track_name.data,
                        ev.u.subscribe_request.track_name.len,
                    )
                )
                found = True
            lib.moq_event_cleanup(ffi.addressof(buf[i]))
        return found

    def accept(self, track_alias: int, now_us):
        """Accept the pending SUBSCRIBE with a pinned track_alias.
        Transitions PENDING_ACCEPT -> ACCEPTED, permanently. Illegal if
        no request has been found yet (call
        poll_for_subscribe_request() first and confirm it returned
        True), or if this session has already accepted one."""
        if self.state != SubscribeAcceptState.PENDING_ACCEPT:
            raise RuntimeError(
                f"accept() is only legal in PENDING_ACCEPT "
                f"(current state: {self.state})"
            )
        if self.sub_handle is None:
            raise RuntimeError(
                "accept() called with no pending SUBSCRIBE_REQUEST -- "
                "call poll_for_subscribe_request() first and confirm it "
                "returned True"
            )
        cfg = ffi.new("moq_accept_subscribe_cfg_t *")
        lib.moq_accept_subscribe_cfg_init(cfg)
        cfg.has_track_alias = True
        cfg.track_alias = track_alias
        _check(
            lib.moq_session_accept_subscribe(
                self.session.ptr, self.sub_handle, cfg, now_us
            ),
            "moq_session_accept_subscribe",
        )
        self.state = SubscribeAcceptState.ACCEPTED

    def send_object_datagram(
        self,
        group_id,
        object_id,
        payload: bytes,
        now_us,
        publisher_priority=0,
        end_of_group=False,
    ):
        """Encode and queue one object datagram against the accepted
        subscription, via the raw moq_session_send_object_datagram
        call under test. Only legal once ACCEPTED. May be called
        multiple times (ACCEPTED is not single-shot -- only the
        PENDING_ACCEPT -> ACCEPTED transition itself is one-way).

        This creates a fresh moq_rcbuf_t from `payload` every call --
        the right choice for a single destination, but wasteful when
        the SAME bytes need to go to multiple sessions (N independent
        allocations and copies of identical data). For that case, see
        make_shared_payload() and send_object_datagram_shared() below.
        """
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"send_object_datagram() is only legal in ACCEPTED "
                f"(current state: {self.state})"
            )
        alloc = default_alloc()
        rcbuf_out = ffi.new("moq_rcbuf_t **")
        _check(
            lib.moq_rcbuf_create(alloc, payload, len(payload), rcbuf_out),
            "moq_rcbuf_create",
        )
        buf = rcbuf_out[0]
        try:
            _check(
                lib.moq_session_send_object_datagram(
                    self.session.ptr,
                    self.sub_handle,
                    group_id,
                    object_id,
                    publisher_priority,
                    end_of_group,
                    buf,
                    ffi.NULL,
                    0,
                    now_us,
                ),
                "moq_session_send_object_datagram",
            )
        finally:
            lib.moq_rcbuf_decref(buf)

    def send_object_datagram_shared(
        self,
        group_id,
        object_id,
        shared_payload,
        now_us,
        publisher_priority=0,
        end_of_group=False,
    ):
        """Like send_object_datagram(), but takes an already-built
        payload handle (from make_shared_payload()) instead of raw
        bytes, so multiple sessions can reuse ONE allocation/copy of
        the same object instead of each independently re-creating an
        identical buffer. Increfs before the send and decrefs after --
        the caller keeps its own reference from make_shared_payload()
        and is responsible for releasing that one separately, once,
        via release_shared_payload(), after every session that needs
        it has been sent to.

        Only safe when every session sharing the same handle lives in
        one shard (thread/event loop): moq_rcbuf_t's own refcounting is
        documented as non-atomic. A relay spread across multiple
        threads would need moq_rcbuf_clone() per destination shard
        instead -- not what this method does.
        """
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"send_object_datagram_shared() is only legal in ACCEPTED "
                f"(current state: {self.state})"
            )
        ref = lib.moq_rcbuf_incref(shared_payload)
        try:
            _check(
                lib.moq_session_send_object_datagram(
                    self.session.ptr,
                    self.sub_handle,
                    group_id,
                    object_id,
                    publisher_priority,
                    end_of_group,
                    ref,
                    ffi.NULL,
                    0,
                    now_us,
                ),
                "moq_session_send_object_datagram",
            )
        finally:
            lib.moq_rcbuf_decref(ref)

    def open_subgroup(self, group_id, subgroup_id, now_us, publisher_priority=0, end_of_group=False):
        """Open a new subgroup on the accepted subscription -- the
        stream-mode (reliable=True) counterpart to
        send_object_datagram(). Returns an opaque subgroup handle;
        write_object() one or more times against it, then
        close_subgroup() when done (typically: one subgroup per Group,
        multiple objects written within it as they arrive, closed when
        the Group ends or a new one starts).

        object_properties is not exposed here -- this binding's usage
        writes plain objects with no properties; extend if a caller
        ever needs them."""
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"open_subgroup() is only legal in ACCEPTED (current state: {self.state})"
            )
        cfg = ffi.new("moq_subgroup_cfg_t *")
        lib.moq_subgroup_cfg_init(cfg)
        cfg.group_id = group_id
        cfg.subgroup_id = subgroup_id
        cfg.publisher_priority = publisher_priority
        cfg.end_of_group = end_of_group
        out = ffi.new("moq_subgroup_handle_t *")
        _check(
            lib.moq_session_open_subgroup(self.session.ptr, self.sub_handle, cfg, now_us, out),
            "moq_session_open_subgroup",
        )
        return out[0]

    def write_object(self, subgroup, object_id, payload: bytes, now_us):
        """Write one object into an already-open subgroup. May be
        called multiple times against the same subgroup handle."""
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"write_object() is only legal in ACCEPTED (current state: {self.state})"
            )
        alloc = default_alloc()
        rcbuf_out = ffi.new("moq_rcbuf_t **")
        _check(
            lib.moq_rcbuf_create(alloc, payload, len(payload), rcbuf_out),
            "moq_rcbuf_create",
        )
        buf = rcbuf_out[0]
        try:
            _check(
                lib.moq_session_write_object(self.session.ptr, subgroup, object_id, buf, now_us),
                "moq_session_write_object",
            )
        finally:
            lib.moq_rcbuf_decref(buf)

    def write_object_shared(self, subgroup, object_id, shared_payload, now_us):
        """Like write_object(), but takes an already-built payload
        handle (from make_shared_payload()) instead of raw bytes --
        the stream-mode counterpart to send_object_datagram_shared(),
        for reusing one encode across N subgroups (across N sessions)
        instead of each independently re-creating an identical buffer.
        Same shard/thread-safety caveat as send_object_datagram_shared()
        applies here identically."""
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"write_object_shared() is only legal in ACCEPTED (current state: {self.state})"
            )
        ref = lib.moq_rcbuf_incref(shared_payload)
        try:
            _check(
                lib.moq_session_write_object(self.session.ptr, subgroup, object_id, ref, now_us),
                "moq_session_write_object",
            )
        finally:
            lib.moq_rcbuf_decref(ref)

    def close_subgroup(self, subgroup, now_us):
        """Close a subgroup previously opened with open_subgroup()."""
        if self.state != SubscribeAcceptState.ACCEPTED:
            raise RuntimeError(
                f"close_subgroup() is only legal in ACCEPTED (current state: {self.state})"
            )
        _check(
            lib.moq_session_close_subgroup(self.session.ptr, subgroup, now_us),
            "moq_session_close_subgroup",
        )

    def destroy(self):
        self.session.destroy()

    def __del__(self):
        try:
            self.destroy()
        except Exception:
            pass
