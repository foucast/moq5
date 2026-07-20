"""
Acceptance test for the moq5 bindings.

Validates that the bindings COMPOSE correctly across a real MoQT
session lifecycle and object-delivery path -- not just that each
wrapped call round-trips in isolation.

Topology: one publisher session emits a known object with known bytes
-> a minimal single-hop relay (one upstream subscriber session polls
it, one downstream publisher session republishes it, using ONLY this
binding's bindings -- no relay/orchestration logic beyond this minimal
wiring) -> one subscriber session receives it. Pass/fail: the received
bytes must exactly match the sent bytes.

Transport: pure in-process byte-passing. MoQ5 is sans-I/O; whatever
one session's poll_actions() produces is handed directly to the peer
session's feed_control()/feed_datagram(). No sockets, no real network
anywhere in this test -- this is the standard way to test a sans-I/O
library in isolation, independent of whatever transport a real
deployment eventually uses.

What this test does NOT validate: whether encoding the same object
against two or more independent downstream sessions produces
byte-identical output. With only one downstream session here, there is
no second session to byte-compare against -- that is a separate,
follow-on question, not a gap in this test.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import moq5


KNOWN_PAYLOAD = (
    b"moq5 binding acceptance test payload -- "
    b"the quick brown fox jumps over the lazy dog, 0123456789."
)

TRACK_NAMESPACE = [b"example"]
TRACK_NAME = b"relaytest"

MAX_ITERATIONS = 200
TICK_STEP_US = 1000  # 1ms per iteration


def pump_hop(session_a: moq5.Session, session_b: moq5.Session, now_us):
    """Drain each session's outbound actions and feed them directly to
    the peer session -- the in-process 'transport' for one hop."""
    for action in session_a.poll_actions():
        if action["kind"] == "send_control":
            session_b.feed_control(action["data"], now_us)
        elif action["kind"] == "send_datagram":
            session_b.feed_datagram(action["data"], now_us)
        elif action["kind"] == "close_session":
            pass  # not expected in the happy path; nothing to forward
        else:
            raise AssertionError(
                f"Unexpected action kind from session_a in happy path: {action}"
            )

    for action in session_b.poll_actions():
        if action["kind"] == "send_control":
            session_a.feed_control(action["data"], now_us)
        elif action["kind"] == "send_datagram":
            session_a.feed_datagram(action["data"], now_us)
        elif action["kind"] == "close_session":
            pass
        else:
            raise AssertionError(
                f"Unexpected action kind from session_b in happy path: {action}"
            )


def run():
    now = 0

    # -- Hop A: upstream publisher (SERVER) <-> relay's subscriber leg (CLIENT)
    upstream_session = moq5.Session(moq5.MOQ_PERSPECTIVE_SERVER, now)
    upstream_pub = moq5.Publisher(upstream_session)
    upstream_track = upstream_pub.add_track(TRACK_NAMESPACE, TRACK_NAME, now)

    relay_sub_session = moq5.Session(moq5.MOQ_PERSPECTIVE_CLIENT, now)
    relay_sub = moq5.Subscriber(relay_sub_session)
    relay_sub_session.start(now)

    # -- Hop B: relay's publisher leg (SERVER) <-> final subscriber (CLIENT)
    relay_pub_session = moq5.Session(moq5.MOQ_PERSPECTIVE_SERVER, now)
    relay_pub = moq5.Publisher(relay_pub_session)
    relay_track = relay_pub.add_track(TRACK_NAMESPACE, TRACK_NAME, now)

    final_sub_session = moq5.Session(moq5.MOQ_PERSPECTIVE_CLIENT, now)
    final_sub = moq5.Subscriber(final_sub_session)
    final_sub_session.start(now)

    relay_track_handle = None
    final_track_handle = None
    published = False
    relayed_payload = None
    relay_written = False
    received_bytes = None

    for iteration in range(MAX_ITERATIONS):
        now += TICK_STEP_US

        # Tick every facade -- drains + dispatches each session's own
        # events internally (see Session.tick's docstring: raw tick and
        # facade tick must never both run on the same session).
        upstream_pub.tick(now)
        relay_sub.tick(now)
        relay_pub.tick(now)
        final_sub.tick(now)

        # Subscribe each leg once its own session reaches ESTABLISHED.
        if relay_track_handle is None and relay_sub_session.state == moq5.MOQ_SESS_ESTABLISHED:
            relay_track_handle = relay_sub.subscribe(TRACK_NAMESPACE, TRACK_NAME, now)

        if final_track_handle is None and final_sub_session.state == moq5.MOQ_SESS_ESTABLISHED:
            final_track_handle = final_sub.subscribe(TRACK_NAMESPACE, TRACK_NAME, now)

        # Upstream publisher writes the known object once it sees a
        # subscriber (the relay's upstream leg) -- exactly once.
        if not published and upstream_pub.has_subscriber(upstream_track):
            upstream_pub.write_object(
                upstream_track,
                group_id=0,
                object_id=0,
                payload=KNOWN_PAYLOAD,
                now_us=now,
                datagram=True,
            )
            published = True

        # Relay: poll the upstream leg's subscriber for the object.
        if relayed_payload is None:
            obj = relay_sub.poll_object()
            if obj is not None and obj["payload"] is not None:
                relayed_payload = obj["payload"]

        # Relay: republish via the downstream leg, once we have the
        # object AND the downstream leg has its own subscriber -- this
        # is the entire "relay logic" in this test: minimal wiring, no
        # orchestration abstraction beyond it.
        if relayed_payload is not None and not relay_written:
            if relay_pub.has_subscriber(relay_track):
                relay_pub.write_object(
                    relay_track,
                    group_id=0,
                    object_id=0,
                    payload=relayed_payload,
                    now_us=now,
                    datagram=True,
                )
                relay_written = True

        # Final subscriber polls for the relayed object.
        if received_bytes is None:
            obj = final_sub.poll_object()
            if obj is not None and obj["payload"] is not None:
                received_bytes = obj["payload"]

        # Pump both hops' in-process "transport".
        pump_hop(upstream_session, relay_sub_session, now)
        pump_hop(relay_pub_session, final_sub_session, now)

        if received_bytes is not None:
            break

    # -- Teardown (best-effort; not part of the pass/fail check) --
    for obj in (
        upstream_pub,
        relay_sub,
        relay_pub,
        final_sub,
        upstream_session,
        relay_sub_session,
        relay_pub_session,
        final_sub_session,
    ):
        obj.destroy()

    if received_bytes is None:
        raise AssertionError(
            f"FAIL: no object received by the final subscriber after "
            f"{MAX_ITERATIONS} iterations ({MAX_ITERATIONS * TICK_STEP_US / 1000:.0f}ms "
            f"virtual time). published={published} relayed_payload_seen="
            f"{relayed_payload is not None} relay_written={relay_written}"
        )

    if received_bytes != KNOWN_PAYLOAD:
        raise AssertionError(
            f"FAIL: byte mismatch.\n  sent     ({len(KNOWN_PAYLOAD)} bytes): "
            f"{KNOWN_PAYLOAD!r}\n  received ({len(received_bytes)} bytes): "
            f"{received_bytes!r}"
        )

    print(
        f"PASS: {len(received_bytes)} bytes relayed through a single-hop "
        f"MoQT relay (upstream publisher -> relay -> final subscriber) "
        f"and matched exactly, after {iteration + 1} iterations "
        f"({(iteration + 1) * TICK_STEP_US / 1000:.0f}ms virtual time)."
    )


if __name__ == "__main__":
    run()
