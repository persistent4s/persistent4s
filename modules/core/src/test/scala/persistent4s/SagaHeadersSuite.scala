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
      SagaHeaders.IdempotencyKey -> "3f2a1c00-0000-0000-0000-000000000001:0:0",
    ),
  )

  pureTest("reply addresses the topic the request nominated") {
    expect(SagaHeaders.reply(request, "ok").map(_.topic) == Some("orders.replies"))
  }

  pureTest("reply echoes the correlation headers the runner routes on") {
    expect(
      SagaHeaders.reply(request, "ok").map(_.headers) == Some(
        Map(
          SagaHeaders.Name -> "reserve-stock",
          SagaHeaders.Id   -> "3f2a1c00-0000-0000-0000-000000000001",
        ),
      ),
    )
  }

  pureTest("reply does not echo the request's idempotency key") {
    // That key identifies the *command*; reusing it on the reply would make two different messages claim one identity.
    expect(SagaHeaders.reply(request, "ok").exists(!_.headers.contains(SagaHeaders.IdempotencyKey)))
  }

  pureTest("reply carries the payload verbatim") {
    expect(SagaHeaders.reply(request, """{"accepted":true}""").map(_.payload) == Some("""{"accepted":true}"""))
  }

  pureTest("reply defaults its key to the request's, keeping one saga on one partition") {
    expect(SagaHeaders.reply(request, "ok").map(_.key) == Some(Some("order-1")))
  }

  pureTest("reply honours an explicit key over the request's") {
    expect(SagaHeaders.reply(request, "ok", key = Some("other")).map(_.key) == Some(Some("other")))
  }

  pureTest("a keyless request yields a keyless reply") {
    expect(SagaHeaders.reply(request.copy(key = None), "ok").map(_.key) == Some(None))
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
