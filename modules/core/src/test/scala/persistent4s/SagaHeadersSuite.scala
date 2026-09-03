/*
 * Copyright 2026 Antonio Jimenez and Bastien Jolidon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package persistent4s

import weaver.SimpleIOSuite

object SagaHeadersSuite extends SimpleIOSuite:

  private val request = IncomingMessage(
    topic = "inventory.commands",
    key = Some("order-1"),
    payload = """{"amount":2}""",
    headers = Map(
      SagaHeaders.Name           -> "reserve-stock",
      SagaHeaders.Id             -> "3f2a1c00-0000-0000-0000-000000000001",
      SagaHeaders.ReplyTo        -> "orders.replies",
      SagaHeaders.IdempotencyKey -> "3f2a1c00-0000-0000-0000-000000000001:0:0:reserve",
    ),
  )

  pureTest("reply addresses the topic the request nominated") {
    expect(SagaHeaders.reply(request, "ok").map(_.topic) == Some("orders.replies"))
  }

  pureTest("reply echoes the correlation headers the runner routes on") {
    expect(
      SagaHeaders.reply(request, "ok").map(_.headers) == Some(
        Map(
          SagaHeaders.Name      -> "reserve-stock",
          SagaHeaders.Id        -> "3f2a1c00-0000-0000-0000-000000000001",
          SagaHeaders.InReplyTo -> "3f2a1c00-0000-0000-0000-000000000001:0:0:reserve",
        ),
      ),
    )
  }

  pureTest("reply does not echo the request's idempotency key") {
    // That key identifies the *command*; reusing it on the reply would make two different messages claim one identity.
    // It comes back under `InReplyTo` instead, which is a reference to the request rather than a second claim on its id.
    expect(SagaHeaders.reply(request, "ok").exists(!_.headers.contains(SagaHeaders.IdempotencyKey)))
  }

  pureTest("reply names the request it answers, so a fan-out saga can tell its replies apart") {
    // Verbatim, label included: the label is the saga's own name for that request and the partner does not know it is
    // carrying one, so anything less than a copy would lose the very thing the saga attributes on.
    expect(
      SagaHeaders.reply(request, "ok").flatMap(_.headers.get(SagaHeaders.InReplyTo)) ==
        Some("3f2a1c00-0000-0000-0000-000000000001:0:0:reserve"),
    )
  }

  pureTest("a request with no idempotency key produces a reply that names nothing") {
    val anonymous = request.copy(headers = request.headers - SagaHeaders.IdempotencyKey)
    expect(SagaHeaders.reply(anonymous, "ok").exists(!_.headers.contains(SagaHeaders.InReplyTo)))
  }

  pureTest("reply carries the partner's own headers alongside the correlation ones") {
    expect(
      SagaHeaders
        .reply(request, "ok", headers = Map("answeredBy" -> "inventory"))
        .flatMap(
          _.headers.get("answeredBy"),
        ) == Some("inventory"),
    )
  }

  pureTest("a partner cannot overwrite a correlation header with one of its own") {
    // The runner routes on these; letting the caller shadow them would make a reply unroutable in a way nothing checks.
    expect(
      SagaHeaders
        .reply(request, "ok", headers = Map(SagaHeaders.Id -> "not-the-instance"))
        .flatMap(
          _.headers.get(SagaHeaders.Id),
        ) == Some("3f2a1c00-0000-0000-0000-000000000001"),
    )
  }

  pureTest("reply carries the payload verbatim") {
    expect(SagaHeaders.reply(request, """{"accepted":true}""").map(_.payload) == Some("""{"accepted":true}"""))
  }

  pureTest("reply defaults its key to the saga instance, keeping one instance's replies on one partition") {
    // Deliberately not the request's key: a request is keyed per resource (an item, a customer), which would scatter one
    // instance's replies across partitions and let two of them be handled at once. Only one such decision can win the
    // step guard, and the loser is discarded.
    expect(SagaHeaders.reply(request, "ok").map(_.key) == Some(Some("3f2a1c00-0000-0000-0000-000000000001")))
  }

  pureTest("reply honours an explicit key over the default") {
    expect(SagaHeaders.reply(request, "ok", key = Some("other")).map(_.key) == Some(Some("other")))
  }

  pureTest("a keyless request still yields a keyed reply") {
    expect(
      SagaHeaders.reply(request.copy(key = None), "ok").map(_.key) ==
        Some(Some("3f2a1c00-0000-0000-0000-000000000001")),
    )
  }

  // A partner may receive commands from senders that are not sagas at all; those have nobody to answer.
  pureTest("no reply is built when the request nominates no reply topic") {
    expect(SagaHeaders.reply(request.copy(headers = request.headers - SagaHeaders.ReplyTo), "ok").isEmpty)
  }

  pureTest("no reply is built when the request carries no saga name") {
    expect(SagaHeaders.reply(request.copy(headers = request.headers - SagaHeaders.Name), "ok").isEmpty)
  }

  pureTest("no reply is built when the request carries no correlation id") {
    expect(SagaHeaders.reply(request.copy(headers = request.headers - SagaHeaders.Id), "ok").isEmpty)
  }

  pureTest("no reply is built for a request with no headers at all") {
    expect(SagaHeaders.reply(request.copy(headers = Map.empty), "ok").isEmpty)
  }

  // ---------------------------------------------------------------------------
  // SagaRequestRef — the format the runner stamps and reads back
  // ---------------------------------------------------------------------------

  private val instance = java.util.UUID.fromString("3f2a1c00-0000-0000-0000-000000000001")

  pureTest("a stamped idempotency key parses back to the request it named") {
    val key = SagaRequestRef.idempotencyKey(instance, round = 2, ordinal = 1, label = "payment")
    expect(SagaRequestRef.parse(key, instance) == Some(SagaRequestRef(2, 1, "payment")))
  }

  pureTest("a key belonging to another instance is not read as one of ours") {
    val other = java.util.UUID.fromString("3f2a1c00-0000-0000-0000-000000000002")
    expect(SagaRequestRef.parse(SagaRequestRef.idempotencyKey(other, 0, 0, "stock"), instance).isEmpty)
  }

  pureTest("a label is free to contain the separator itself") {
    // The label is the remainder of the key, not a fourth field, so a saga is not quietly constrained in what it may
    // call its own requests — and a label that did get truncated would attribute a reply to nothing.
    val key = SagaRequestRef.idempotencyKey(instance, 0, 0, "payment:authorize")
    expect(SagaRequestRef.parse(key, instance).map(_.label) == Some("payment:authorize"))
  }

  pureTest("an empty label survives the round trip") {
    // Nothing forbids one, and it has to read back as the empty label rather than as an unparseable key: a saga that
    // labels nothing still needs its single reply attributed.
    val key = SagaRequestRef.idempotencyKey(instance, 3, 0, "")
    expect(SagaRequestRef.parse(key, instance) == Some(SagaRequestRef(3, 0, "")))
  }

  pureTest("a malformed key yields nothing rather than a plausible-looking ref") {
    expect.all(
      SagaRequestRef.parse("nonsense", instance).isEmpty,
      SagaRequestRef.parse(s"$instance:0", instance).isEmpty,
      SagaRequestRef.parse(s"$instance:zero:0", instance).isEmpty,
      // The pre-label format. It is no longer a key this runner could have stamped, and reading it as a label-less ref
      // would hand a fan-out an answer it has no way to attribute anyway.
      SagaRequestRef.parse(s"$instance:0:0", instance).isEmpty,
      SagaRequestRef.parse(s"$instance:0:zero:label", instance).isEmpty,
    )
  }

  // ---------------------------------------------------------------------------
  // RequestContext — what a partner can tell about a request it has just picked up
  // ---------------------------------------------------------------------------

  private val now = java.time.Instant.parse("2026-08-06T12:00:00Z")

  private def received(headers: Map[String, String]): RequestContext =
    RequestContext(request.copy(headers = request.headers ++ headers), now)

  pureTest("a request with no expiry never expires") {
    // The plain fire-and-forget case: a caller that set no deadline is not waiting, so there is nothing to be late for.
    expect(!received(Map.empty).hasExpired)
  }

  pureTest("a request whose expiry is still ahead has not expired") {
    expect(!received(Map(SagaHeaders.ExpiresAt -> now.plusSeconds(30).toString)).hasExpired)
  }

  pureTest("a request whose expiry has gone by has expired") {
    expect(received(Map(SagaHeaders.ExpiresAt -> now.minusSeconds(1).toString)).hasExpired)
  }

  pureTest("a request expiring exactly now has not expired yet") {
    // The boundary belongs to the caller: `receivedAt` has to be strictly after the deadline, so a request that lands
    // on its own expiry instant is still in time.
    expect(!received(Map(SagaHeaders.ExpiresAt -> now.toString)).hasExpired)
  }

  pureTest("a request whose expiry cannot be read counts as expired") {
    // The one case worth stating twice. The header exists to bound staleness; if it cannot be parsed then staleness is
    // unknown, and honouring the request anyway would restore exactly the leak the header was added to narrow.
    expect(received(Map(SagaHeaders.ExpiresAt -> "not an instant")).hasExpired)
  }

  pureTest("a request carrying the saga correlation headers is addressed") {
    expect(received(Map.empty).isAddressed)
  }

  pureTest("a request missing any correlation header is not addressed") {
    val bare = RequestContext(request.copy(headers = Map.empty), now)
    val partial = RequestContext(request.copy(headers = request.headers - SagaHeaders.ReplyTo), now)
    expect.all(!bare.isAddressed, !partial.isAddressed)
  }
