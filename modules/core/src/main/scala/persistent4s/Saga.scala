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

import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.concurrent.duration.FiniteDuration

/** Identifies the saga instance a decision is being made for, so decision functions can build tags and messages that
  * reference the instance without carrying the key around in the state themselves.
  */
final case class SagaContext(id: UUID, sagaName: String, key: String, step: Int)

/** A command a saga wants to send, still typed. The runner encodes it with [[Saga.requestCodec]] and stamps the
  * [[SagaHeaders]] onto it, so a saga never serializes anything itself — its decision functions are pure and would have
  * nowhere to report an encoding failure.
  *
  * @param key
  *   partition key for the request; drives per-key ordering at the broker
  * @param headers
  *   extra headers to send alongside the saga's own; the saga headers win on a clash
  */
final case class SagaRequest[+Req](
  topic: String,
  key: Option[String],
  payload: Req,
  headers: Map[String, String] = Map.empty,
)

/** What a saga does when it recognises a trigger event: which instance to create, with what state, and which command(s)
  * to send.
  *
  * @param key
  *   correlation key, unique per instance within this saga.
  * @param timeout
  *   how long to wait for a reply before [[Saga.onTimeout]] fires; `None` waits forever
  */
final case class SagaStart[S, +Req](
  key: String,
  data: S,
  request: List[SagaRequest[Req]],
  timeout: Option[FiniteDuration] = None,
)

/** Where a saga instance goes after handling a reply or a deadline. */
enum SagaOutcome[+S]:

  case Continue(data: S, timeout: Option[FiniteDuration])

  case Completed

  case Compensated

  case Failed(reason: String)

/** The events to append and commands to send as a result of a reply or a timeout, plus the instance's next state. */
final case class SagaDecision[A <: Event, +S, +Req](
  outcome: SagaOutcome[S],
  events: List[(Set[Tag], A)],
  messages: List[SagaRequest[Req]],
)

object SagaDecision:

  def completed[A <: Event, Req](
    events: List[(Set[Tag], A)] = Nil,
    messages: List[SagaRequest[Req]] = Nil,
  ): SagaDecision[A, Nothing, Req] = SagaDecision(SagaOutcome.Completed, events, messages)

  def compensated[A <: Event, Req](
    events: List[(Set[Tag], A)] = Nil,
    messages: List[SagaRequest[Req]] = Nil,
  ): SagaDecision[A, Nothing, Req] = SagaDecision(SagaOutcome.Compensated, events, messages)

  def failed[A <: Event, Req](
    reason: String,
    events: List[(Set[Tag], A)] = Nil,
    messages: List[SagaRequest[Req]] = Nil,
  ): SagaDecision[A, Nothing, Req] = SagaDecision(SagaOutcome.Failed(reason), events, messages)

  def continue[A <: Event, S, Req](
    data: S,
    timeout: Option[FiniteDuration],
    events: List[(Set[Tag], A)] = Nil,
    messages: List[SagaRequest[Req]] = Nil,
  ): SagaDecision[A, S, Req] = SagaDecision(SagaOutcome.Continue(data, timeout), events, messages)

/** Headers the runner attaches to every saga request message. The partner must echo [[Name]] and [[Id]] back on its
  * reply so the runner can route it to the right saga and instance, and must publish that reply to [[ReplyTo]].
  */
object SagaHeaders:

  val Name = "persistent4s.sagaName"

  val Id = "persistent4s.sagaId"

  /** De-duplication key for the command being sent. A partner that may receive the same command twice — the outbox
    * guarantees at-least-once — should treat a repeat of this key as already handled.
    */
  val IdempotencyKey = "persistent4s.idempotencyKey"

  /** Topic the partner must publish its reply to. Carrying the address on the message keeps a partner from hardcoding
    * any one caller's topology, so several services can send it the same command and each get answered.
    */
  val ReplyTo = "persistent4s.replyTo"

  /** Build the reply to a saga request: `payload` addressed to the topic the request nominated, carrying back the
    * correlation headers [[SagaRunner]] needs to route it to the right saga and instance.
    *
    * `None` when `request` did not come from a saga — no [[ReplyTo]], [[Name]] or [[Id]] — in which case there is
    * nobody to answer and the caller should treat the command as fire-and-forget.
    *
    * `payload` is already encoded, unlike [[SagaRequest]]: a partner answers inside an effect and can report an
    * encoding failure itself, whereas a saga's decision functions are pure and cannot.
    *
    * @param key
    *   partition key for the reply; defaults to the request's own, keeping one saga's traffic on one partition
    */
  def reply(request: IncomingMessage, payload: String, key: Option[String] = None): Option[OutgoingMessage] =
    for
      replyTo <- request.headers.get(ReplyTo)
      name    <- request.headers.get(Name)
      id      <- request.headers.get(Id)
    yield OutgoingMessage(
      topic = replyTo,
      key = Some(key.getOrElse(id)),
      payload = payload,
      headers = Map(Name -> name, Id -> id),
    )

/** Deterministic identifiers, so that replaying a trigger event or redelivering a reply produces the same ids and
  * results in a no-op.
  */
object SagaId:

  def instance(sagaName: String, key: String): UUID =
    UUID.nameUUIDFromBytes(s"$sagaName:$key".getBytes(StandardCharsets.UTF_8))

  def event(id: UUID, step: Int, ordinal: Int): UUID =
    UUID.nameUUIDFromBytes(s"$id:$step:$ordinal".getBytes(StandardCharsets.UTF_8))

/** A saga coordinates work that cannot be committed in a single local transaction: it reacts to a trigger event in this
  * service's own log by sending a command message to another service, waits for that service's reply, and then either
  * completes or compensates.
  *
  * @tparam A
  *   the event type of this service's event store
  * @tparam S
  *   state carried by an instance between its request and the reply
  * @tparam Req
  *   the command(s) this saga sends. A multi-step saga makes this a sealed trait covering everything it can ask for.
  * @tparam Rep
  *   the reply payload this saga expects
  */
trait Saga[A <: Event, S, Req, Rep]:

  /** The name of this saga.
    *
    * ⚠️ **STABLE IDENTIFIER** — it names the trigger loop's checkpoint, the leader-election lease, and the
    * `persistent4s.sagaName` header partners echo back. Changing it in production orphans in-flight instances and the
    * checkpoint, and requires a manual migration.
    */
  def name: String

  /** Event types that may start an instance. Only these are read by the trigger loop. */
  def triggers: Set[EventTypeName]

  /** Decide whether `event` starts an instance. Returning `None` skips it and the checkpoint still advances past it. */
  def start(event: EventEnvelope[A]): Option[SagaStart[S, Req]]

  /** Interpret the partner's reply for a pending instance. */
  def onReply(ctx: SagaContext, state: S, reply: Rep): SagaDecision[A, S, Req]

  /** Decide what to do when a pending instance passes its deadline — normally a compensation. */
  def onTimeout(ctx: SagaContext, state: S): SagaDecision[A, S, Req]

  /** Serializes [[S]]; the runner persists instance state as text. */
  def stateCodec: MessageCodec[S]

  /** Encodes the commands this saga sends. */
  def requestCodec: MessageCodec[Req]

  /** Decodes reply payloads into [[Rep]]. */
  def replyCodec: MessageCodec[Rep]
