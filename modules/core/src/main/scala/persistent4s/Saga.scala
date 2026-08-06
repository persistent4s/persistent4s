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
import scala.util.Try

/** What a decision does to the instance's deadline.
  *
  * Three cases rather than an `Option[FiniteDuration]`, because "wait another 30 seconds" and "keep the 30 seconds you
  * were already given" are different instructions and a duration can only say the first. A fan-in that re-armed on
  * every partial reply would hand a slow partner an unbounded extension, one reply at a time.
  *
  * Named with the `Saga` prefix, unlike most types that live only inside one API: every saga file needs
  * `scala.concurrent.duration.*` for its durations, and that brings a `Deadline` of its own. Two wildcard imports
  * offering one name is an ambiguity rather than a shadowing, so a bare `Deadline` here would fail to compile in
  * exactly the files that use it most.
  */
enum SagaDeadline:

  /** Leave the deadline exactly where it is. */
  case Keep

  /** Wait indefinitely: the timer loop never claims this instance. */
  case Never

  /** Give the instance `duration` from the moment this decision is applied. */
  case In(duration: FiniteDuration)

/** Identifies the saga instance a decision is being made for, so decision functions can build tags and messages that
  * reference the instance without carrying the key around in the state themselves.
  */
final case class SagaContext(id: UUID, sagaName: String, key: String, step: Int)

/** What a request type is called on the wire.
  *
  * The name is the contract between a saga and the service it asks, and it used to be kept by hand in three places at
  * once: the header the sender stamped, the encoder that matched on the payload to serialize it, and the dispatch table
  * the partner matched on to route it. Three agreements, checked by nobody — stamp one name beside another's payload
  * and the message is silently dropped by the other service.
  *
  * Declared once, next to the type it names, and both sides read it from there:
  *
  * ```scala
  * final case class ReserveStock(orderId: UUID, itemId: UUID, amount: Int)
  *
  * object ReserveStock:
  *   given RequestType[ReserveStock] = RequestType("reserve")
  *   given MessageCodec[ReserveStock] = CirceMessageCodec.derived
  * ```
  *
  * Deliberately '''only''' the name. The sender needs an encoder and the receiver a decoder, and per
  * [[MessageEncoder]]'s own reasoning a service often cannot supply the direction it does not use — so each side asks
  * for this plus its own half, rather than both being made to provide a full codec.
  *
  * ⚠️ **STABLE IDENTIFIER** — it goes on the wire as [[SagaHeaders.RequestType]]. Renaming it while requests are in
  * flight means the partner stops recognising them.
  */
trait RequestType[A]:

  def name: String

object RequestType:

  def apply[A](name: String): RequestType[A] =
    val declared = name
    new RequestType[A]:
      val name: String = declared

/** A command a saga wants to send, named rather than serialized.
  *
  * Like [[PendingReply]], and for the same reason: a saga's decision functions are pure and have nowhere to report an
  * encoding failure, so the payload is captured with its encoder here and turned into bytes by [[SagaRunner]], where a
  * failure can be raised. Capturing it at construction is also what lets one saga send several unrelated request types
  * without hand-writing an encoder that matches on all of them.
  *
  * @param label
  *   names this request. Must be distinct within a round: two requests sharing a label have indistinguishable replies.
  *   It travels inside [[SagaHeaders.IdempotencyKey]] and comes back on [[SagaHeaders.InReplyTo]], so a partner
  *   replying with [[SagaHeaders.reply]] returns it without knowing it exists.
  * @param key
  *   partition key for the request; drives per-key ordering at the broker
  * @param headers
  *   extra headers to send alongside the saga's own; the saga headers win on a clash
  */
sealed abstract class SagaRequest[+Req]:

  def label: String

  def topic: String

  def key: Option[String]

  def payload: Req

  /** What the partner will see in [[SagaHeaders.RequestType]], taken from the payload's own [[RequestType]]. */
  def requestType: String

  def headers: Map[String, String]

  /** The payload in wire form, or why it could not be produced. */
  private[persistent4s] def encoded: Either[Throwable, String]

object SagaRequest:

  private final case class Deferred[+Req](
    label: String,
    topic: String,
    key: Option[String],
    payload: Req,
    requestType: String,
    headers: Map[String, String],
    encode: () => Either[Throwable, String],
  ) extends SagaRequest[Req]:

    def encoded: Either[Throwable, String] = encode()

  def apply[A](
    label: String,
    topic: String,
    key: Option[String],
    payload: A,
    headers: Map[String, String] = Map.empty,
  )(using requestType: RequestType[A], encoder: MessageEncoder[A]): SagaRequest[A] =
    Deferred(label, topic, key, payload, requestType.name, headers, () => encoder.encode(payload))

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

  case Continue(data: S, deadline: SagaDeadline)

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
    deadline: SagaDeadline = SagaDeadline.Keep,
    events: List[(Set[Tag], A)] = Nil,
    messages: List[SagaRequest[Req]] = Nil,
  ): SagaDecision[A, S, Req] = SagaDecision(SagaOutcome.Continue(data, deadline), events, messages)

/** Identifies one of the requests an instance has sent: the emission round it went out in, its position within that
  * round, and the label the saga gave it.
  *
  * The label is what a fan-out attributes on: it is chosen by the saga rather than derived from where the request sat
  * in the list, so reordering that list cannot silently move an answer from one partner to another. The ordinal stays
  * because it is what keeps the key unique even when a saga does label two requests the same.
  */
final case class SagaRequestRef(round: Int, ordinal: Int, label: String)

object SagaRequestRef:

  /** The single definition of the [[SagaHeaders.IdempotencyKey]] format. [[SagaRunner]] stamps requests with this and
    * [[parse]] reads it back off [[SagaHeaders.InReplyTo]], so the producer and the reader cannot drift — a second
    * spelling of the format would show up only as [[SagaReply.answering]] quietly being `None` forever.
    */
  def idempotencyKey(instance: UUID, round: Int, ordinal: Int, label: String): String =
    s"$instance:$round:$ordinal:$label"

  /** `None` unless `key` is well-formed '''and''' belongs to `expected`, so a key echoed from somewhere else cannot be
    * mistaken for one of this instance's own requests.
    */
  def parse(key: String, expected: UUID): Option[SagaRequestRef] =
    key.split(":", 4) match
      case Array(instance, round, ordinal, label) if instance == expected.toString =>
        for
          r <- Try(round.toInt).toOption
          o <- Try(ordinal.toInt).toOption
        yield SagaRequestRef(r, o, label)
      case _ => None

/** A partner's answer, with everything the runner knows about how it arrived.
  *
  * An envelope rather than a bare `Rep` for the same reason [[Saga.start]] receives an [[EventEnvelope]]: what a
  * decision function needs is the payload *and* its provenance. It also means the next thing worth telling a saga about
  * a reply does not change [[Saga.onReply]]'s signature again.
  *
  * @param answering
  *   which request this answers, taken from [[SagaHeaders.InReplyTo]] — [[SagaHeaders.reply]] sets it automatically.
  *   `None` if the partner built its reply by hand, or if the key it named was not one of this instance's requests.
  * @param headers
  *   the reply message's headers, unfiltered, `persistent4s.*` ones included. A partner that prefers to describe its
  *   own replies rather than rely on [[answering]] can put whatever it likes here.
  */
final case class SagaReply[+Rep](
  payload: Rep,
  answering: Option[SagaRequestRef],
  headers: Map[String, String],
)

/** A reply a partner wants sent, named rather than serialized.
  *
  * Mirrors [[PendingEvent]] one layer down: a complete value that is not yet in wire form. That matters because the
  * decision to reply is made in pure code — [[SagaCommandHandler.reply]] sees only the state, the command and the
  * outcome — and pure code has nowhere to report an encoding failure. Naming the payload and deferring its encoding
  * moves that failure somewhere it can be raised rather than thrown.
  *
  * Note what is '''absent''': the reply's partition key. It defaults to the saga instance, and the only safe
  * alternatives are values that are also per-instance, so offering the choice buys nothing and costs a great deal when
  * it is made wrongly — two partners picking differently scatter one instance's replies across partitions, where they
  * are handled at once and the loser of [[SagaRepository.advance]]'s guard is discarded. A partner with a genuine
  * reason can still build the message itself with [[SagaHeaders.reply]].
  *
  * @param headers
  *   extra headers to send alongside the correlation ones; the correlation headers win on a clash.
  */
sealed abstract class PendingReply:

  def headers: Map[String, String]

  /** The payload in wire form, or why it could not be produced. */
  private[persistent4s] def encoded: Either[Throwable, String]

object PendingReply:

  private final case class Deferred(
    headers: Map[String, String],
    encode: () => Either[Throwable, String],
  ) extends PendingReply:

    def encoded: Either[Throwable, String] = encode()

  def apply[A](payload: A, headers: Map[String, String] = Map.empty)(using
    encoder: MessageEncoder[A],
  ): PendingReply = Deferred(headers, () => encoder.encode(payload))

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

  /** On a reply: the [[IdempotencyKey]] of the request being answered.
    *
    * A separate header rather than echoing [[IdempotencyKey]] itself, because that key is the identity of ''one''
    * message — copying it onto the reply would have two different messages claiming the same one. This says "I am the
    * answer to that", which is a reference, not an identity, and it is what [[SagaReply.answering]] is read from.
    */
  val InReplyTo = "persistent4s.inReplyTo"

  /** When the caller stops waiting, as an ISO-8601 instant. Stamped from the instance's own deadline, so a partner and
    * the saga that asked it share one fact rather than each holding an opinion about how stale the request is.
    */
  val ExpiresAt = "persistent4s.expiresAt"

  /** Which of the partner's commands this is, from the payload's [[RequestType]]. It is what
    * [[SagaParticipant.on]] routes on, and the reason neither side has to write the name down twice.
    */
  val RequestType = "persistent4s.requestType"

  /** Build the reply to a saga request: `payload` addressed to the topic the request nominated, carrying back the
    * correlation headers [[SagaRunner]] needs to route it to the right saga and instance.
    *
    * `None` when `request` did not come from a saga — no [[ReplyTo]], [[Name]] or [[Id]] — in which case there is
    * nobody to answer and the caller should treat the command as fire-and-forget.
    *
    * `payload` is already encoded, unlike [[SagaRequest]]: a partner answers inside an effect and can report an
    * encoding failure itself, whereas a saga's decision functions are pure and cannot.
    *
    * The request's [[IdempotencyKey]] comes back as [[InReplyTo]] when it has one, which is what lets the runner tell
    * the saga ''which'' of its requests is being answered — see [[SagaReply.answering]]. A partner gets that for free
    * by using this method; one that builds its own reply has to set that header itself or the saga learns nothing.
    *
    * @param key
    *   partition key for the reply; defaults to the saga instance id, which is what keeps one instance's replies on one
    *   partition and therefore handled one at a time. Override only with something equally per-instance: two replies to
    *   the same instance handled concurrently race each other, and the loser is discarded.
    * @param headers
    *   extra headers to send alongside the correlation ones; the correlation headers win on a clash, as with
    *   [[SagaRequest.headers]]
    */
  def reply(
    request: IncomingMessage,
    payload: String,
    key: Option[String] = None,
    headers: Map[String, String] = Map.empty,
  ): Option[OutgoingMessage] =
    for
      replyTo <- request.headers.get(ReplyTo)
      name    <- request.headers.get(Name)
      id      <- request.headers.get(Id)
    yield OutgoingMessage(
      topic = replyTo,
      key = Some(key.getOrElse(id)),
      payload = payload,
      headers = headers ++ Map(Name -> name, Id -> id) ++
        request.headers.get(IdempotencyKey).map(InReplyTo -> _),
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

  /** Interpret the partner's reply for a pending instance.
    *
    * A saga that sent more than one request tells them apart with [[SagaReply.answering]], which identifies the request
    * being answered by round and ordinal — the same order they were listed in.
    */
  def onReply(ctx: SagaContext, state: S, reply: SagaReply[Rep]): SagaDecision[A, S, Req]

  /** Decide what to do when a pending instance passes its deadline — normally a compensation. */
  def onTimeout(ctx: SagaContext, state: S): SagaDecision[A, S, Req]

  /** Serializes [[S]]; the runner persists instance state as text and reads it back on every reply and deadline, so
    * this is the one payload here that genuinely needs both directions.
    */
  def stateCodec: MessageCodec[S]

  /** Decodes reply payloads into [[Rep]]. */
  def replyDecoder: MessageDecoder[Rep]
