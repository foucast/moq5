"""
Acceptance test for pinned track_alias byte-identity.

Checks the specific claim: does pinning the same track_alias across
two independently-accepted downstream sessions make
moq_session_send_object_datagram() produce byte-identical output for
the same object? This was reasoned from the draft-16 spec text and
MoQ5's C API signatures (only track_alias is caller-controllable and
session-scoped in an OBJECT_DATAGRAM), but never checked against real
encoded bytes until now.

Topology: two independent (RawAcceptSession, peer Session) pairs, each
going through its own real CLIENT_SETUP/SERVER_SETUP handshake and a
real SUBSCRIBE from the peer. Each RawAcceptSession polls raw events to
find the SUBSCRIBE_REQUEST, then accepts it with a pinned track_alias
-- entirely at the raw session level; see RawAcceptSession's own
docstring for why no Publisher facade is used here (the facade cannot
be retroactively wired to a subscription accepted via the raw path --
confirmed directly against publisher.h, which has no function taking a
moq_subscription_t or track_alias anywhere).

Two checks:
  1. POSITIVE: both sessions pinned to the SAME track_alias. Encode the
     same object against each. The resulting bytes must be identical.
  2. NEGATIVE CONTROL: both sessions pinned to DIFFERENT track_alias
     values. Encode the same object against each. The resulting bytes
     must DIFFER. Without this, check 1 passing could mean either "the
     mechanism works" or "the comparison is vacuously insensitive to
     alias" (e.g. a bug that always produces the same fixed bytes
     regardless of input) -- this rules out the latter explicitly, the
     same way a deliberate-corruption check on a different acceptance
     test elsewhere in this project ruled out that test being vacuous.

If check 1 fails: the underlying reasoning (track_alias is the
only session-scoped OBJECT_DATAGRAM field) is wrong or incomplete --
something else varies per session that wasn't caught by reading the
spec text, and needs to be found by diffing the two byte strings
directly before any downstream design relies on the shared-payload
assumption.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import moq5


TRACK_NAMESPACE = [b"example"]
TRACK_NAME = b"relaytest"

MAX_ITERATIONS = 200
TICK_STEP_US = 1000  # 1ms per iteration


def pump_hop(session_a, session_b, now_us):
    """Same in-process transport pattern as the single-hop test:
    drain each side's outbound actions, feed control bytes directly to
    the peer. Only SEND_CONTROL is expected here -- the handshake and
    SUBSCRIBE exchange are control-plane only; datagram encoding
    happens locally on the accepting side after the handshake settles
    and is never fed to the peer at all (this test inspects the
    encoded bytes directly, it doesn't need the peer to receive them)."""
    for session, peer in ((session_a, session_b), (session_b, session_a)):
        for action in session.poll_actions():
            if action["kind"] == "send_control":
                peer.feed_control(action["data"], now_us)
            elif action["kind"] == "close_session":
                pass
            else:
                raise AssertionError(
                    f"Unexpected action kind in happy path: {action}"
                )


def establish_pinned_session(track_alias: int):
    """Build one (RawAcceptSession, peer) pair: real handshake, real
    SUBSCRIBE from the peer, real accept with the given pinned
    track_alias. Returns (acceptor, peer, now) -- the ACCEPTED
    RawAcceptSession, its peer, and the virtual clock value reached, so
    the caller can continue advancing time from where this left off
    rather than jumping to an arbitrary disconnected timestamp. Raises
    AssertionError if the pair doesn't reach ACCEPTED within
    MAX_ITERATIONS."""
    now = 0
    acceptor = moq5.RawAcceptSession(moq5.MOQ_PERSPECTIVE_SERVER, now)
    peer = moq5.Session(moq5.MOQ_PERSPECTIVE_CLIENT, now)
    peer.start(now)

    subscribed = False

    for _ in range(MAX_ITERATIONS):
        now += TICK_STEP_US
        acceptor.tick(now)
        peer.tick(now)

        if not subscribed and peer.state == moq5.MOQ_SESS_ESTABLISHED:
            peer.raw_subscribe(TRACK_NAMESPACE, TRACK_NAME, now)
            subscribed = True

        if acceptor.poll_for_subscribe_request():
            acceptor.accept(track_alias, now)
            pump_hop(acceptor.session, peer, now)
            return acceptor, peer, now

        pump_hop(acceptor.session, peer, now)

    raise AssertionError(
        f"FAIL: session never reached ACCEPTED within {MAX_ITERATIONS} "
        f"iterations (subscribed={subscribed})"
    )


def encode_datagram(acceptor, group_id, object_id, payload: bytes, now_us):
    """Call send_object_datagram, then immediately drain poll_actions
    for the resulting SEND_DATAGRAM action's bytes. Raises
    AssertionError if no SEND_DATAGRAM action appears -- draft-16's
    shared control stream means nothing else should be queued at this
    point."""
    acceptor.send_object_datagram(group_id, object_id, payload, now_us)
    for action in acceptor.poll_actions():
        if action["kind"] == "send_datagram":
            return action["data"]
    raise AssertionError(
        "FAIL: send_object_datagram() did not produce a SEND_DATAGRAM "
        "action to poll"
    )


def run_pair(alias_a: int, alias_b: int):
    """Set up two independent accepted sessions pinned to the given
    (possibly equal, possibly different) aliases, encode the same
    object against each, and return the two resulting byte strings."""
    acceptor_a, peer_a, now_a = establish_pinned_session(alias_a)
    acceptor_b, peer_b, now_b = establish_pinned_session(alias_b)

    payload = b"track-alias-pinning acceptance test payload, 0123456789."
    encoded_a = encode_datagram(acceptor_a, group_id=42, object_id=7, payload=payload, now_us=now_a + TICK_STEP_US)
    encoded_b = encode_datagram(acceptor_b, group_id=42, object_id=7, payload=payload, now_us=now_b + TICK_STEP_US)

    for obj in (acceptor_a, peer_a, acceptor_b, peer_b):
        obj.destroy()

    return encoded_a, encoded_b


def run():
    # -- Check 1: POSITIVE -- same alias pinned on both sessions --
    same_alias = 4242
    encoded_a, encoded_b = run_pair(same_alias, same_alias)

    if encoded_a != encoded_b:
        raise AssertionError(
            f"FAIL (positive case): pinning the same track_alias "
            f"({same_alias}) on both sessions produced DIFFERENT encoded "
            f"bytes.\n  session A ({len(encoded_a)} bytes): {encoded_a.hex()}"
            f"\n  session B ({len(encoded_b)} bytes): {encoded_b.hex()}\n"
            f"This means track_alias is NOT the only session-scoped "
            f"OBJECT_DATAGRAM field -- diff the two byte strings to find "
            f"what else varies before relying on the shared-payload design."
        )

    print(
        f"PASS (positive): {len(encoded_a)} bytes, identical across two "
        f"independently-accepted sessions pinned to the same track_alias "
        f"({same_alias})."
    )

    # -- Check 2: NEGATIVE CONTROL -- different aliases --
    encoded_c, encoded_d = run_pair(1111, 2222)

    if encoded_c == encoded_d:
        raise AssertionError(
            "FAIL (negative control): pinning DIFFERENT track_alias "
            "values (1111 vs 2222) produced IDENTICAL encoded bytes. "
            "This means the positive check above is not actually "
            "sensitive to track_alias -- it may be passing vacuously "
            "(e.g. a bug that always emits the same fixed bytes "
            "regardless of input), which would make the positive result "
            "meaningless."
        )

    print(
        f"PASS (negative control): pinning different track_alias values "
        f"(1111 vs 2222) produced DIFFERENT encoded bytes ({len(encoded_c)} "
        f"vs {len(encoded_d)} bytes) -- confirms the positive check above "
        f"is actually sensitive to track_alias, not vacuous."
    )

    print("\nOVERALL: PASS -- track_alias pinning produces byte-identical "
          "OBJECT_DATAGRAM encoding across independent sessions, and the "
          "comparison is confirmed sensitive to the mechanism under test.")


if __name__ == "__main__":
    run()
