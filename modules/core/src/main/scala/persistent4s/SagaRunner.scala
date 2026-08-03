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

import java.util.UUID

import scala.util.Try
import scala.concurrent.duration.*

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger

/** Drives a [[Saga]]. Three independent loops advance an instance through its life:
  *
  *   - the '''trigger loop''' reads this service's own event log through a checkpoint and starts instances;
  *   - the '''reply loop''' consumes `replyTopic` and applies partners' answers;
  *   - the '''timer loop''' claims instances whose deadline has passed and compensates them.
  *
  * '''Only the trigger loop is a singleton.''' It shares one checkpoint, so two of them would race it and skip events —
  * [[run]] holds a [[LeaderElection]] lease for it. The other two need no lease: the reply loop is a consumer group, so
  * running it everywhere is how it scales, and [[SagaRepository.claimExpired]] is already safe against concurrent
  * claimers.
  *
  * @param store
  *   this service's event store; must be able to enqueue messages in the appending transaction
  * @param replyTopic
  *   where partners are told to send replies, stamped onto every request as [[SagaHeaders.ReplyTo]]
  */
final case class SagaRunner[F[_]: Async: Logger, A <: Event] private (
  store: EventStore[F, A] & EventNotification[F] & TransactionalMessages[F, A],
  checkpoint: ProjectionCheckpoint[F],
  repository: SagaRepository[F],
  replies: MessageSubscriber[F],
  replyTopic: String,
  batchSize: Int,
  timerPollInterval: FiniteDuration,
  timerBatchSize: Int,
  maxDrainPasses: Int,
  replyFromBeginning: Boolean,
):

  import SagaRunner.*

  def triggerLoop[S, Req, Rep](saga: Saga[A, S, Req, Rep]): Stream[F, Unit] =
    Projector(store, checkpoint, batchSize).run(triggerProjection(saga))

  private def triggerProjection[S, Req, Rep](saga: Saga[A, S, Req, Rep]): StatelessProjection[F, A] =
    new StatelessProjection[F, A]:
      val name: String = loopName(saga.name)
      val filter: Set[EventTypeName] = saga.triggers
      def handle(event: EventEnvelope[A]): F[Unit] = startInstance(saga, event)

  private def startInstance[S, Req, Rep](saga: Saga[A, S, Req, Rep], event: EventEnvelope[A]): F[Unit] =
    saga.start(event) match
      case None        => Async[F].unit
      case Some(start) =>
        val id = SagaId.instance(saga.name, start.key)
        for
          data     <- encodeState(saga, start.data)
          requests <- encodeRequests(saga, id, step = 0, start.request)
          started  <- repository.start(id, saga.name, start.key, data, start.timeout, requests)
          _        <-
            if started then Logger[F].debug(s"saga '${saga.name}' started instance $id for key '${start.key}'")
            else Logger[F].debug(s"saga '${saga.name}' instance $id already exists, skipping")
        yield ()

  /** Append a decision's events and enqueue its commands atomically, then move the instance.
    *
    * The order is deliberate. Appending first means a crash before [[SagaRepository.advance]] replays harmlessly — the
    * events carry deterministic ids and collapse on re-insert, and the commands carry an idempotency key. Advancing
    * first would risk a terminal instance whose events were never written, which nothing would ever retry.
    *
    * The flip side of that order is that two decisions reaching the same instance at the same step — a deadline firing
    * just as a reply lands — both emit before either advances, and only one then wins the guard. Their emissions share
    * a round and so share ids and idempotency keys, meaning the loser's collapse into the winner's rather than doubling
    * up. Which of the two reached the partner first need not be the one that won the guard.
    */
  private def applyDecision[S, Req, Rep](
    saga: Saga[A, S, Req, Rep],
    record: SagaRecord,
    decision: SagaDecision[A, S, Req],
  ): F[Unit] =
    val (status, nextStep, nextData, timeout) = decision.outcome match
      case SagaOutcome.Continue(data, t) => (SagaStatus.Pending, record.step + 1, Some(data), t)
      case SagaOutcome.Completed         => (SagaStatus.Completed, record.step, None, None)
      case SagaOutcome.Compensated       => (SagaStatus.Compensated, record.step, None, None)
      case SagaOutcome.Failed(_)         => (SagaStatus.Failed, record.step, None, None)

    // Emissions are keyed by round, not by the stored step. `startInstance` already used round 0, so a terminal
    // decision keeping `record.step` would hand a compensating command the same idempotency key as the original
    // request — and a partner deduplicating on that key would drop the compensation.
    val emissionRound = record.step + 1

    for
      data     <- nextData.fold(Async[F].pure(record.data))(encodeState(saga, _))
      requests <- encodeRequests(saga, record.id, emissionRound, decision.messages)
      events    = decision.events.zipWithIndex.map { case ((tags, event), ordinal) =>
                 PendingEvent(
                   payload = event,
                   tags = tags,
                   eventType = EventTypeName.fromInstance(event),
                   isExternal = false,
                   id = Some(SagaId.event(record.id, emissionRound, ordinal)),
                   headers = Map(SagaHeaders.Name -> saga.name, SagaHeaders.Id -> record.id.toString),
                 )
               }
      _     <- store.appendUncheckedWithMessages(requests, events)
      moved <- repository.advance(record.id, record.step, status, nextStep, data, timeout)
      _     <-
        if !moved then
          Logger[F].warn(s"saga '${saga.name}' instance ${record.id} was already advanced, decision discarded")
        else
          decision.outcome match
            // The reason is the one thing an operator wants when they find a Failed instance, and it is not persisted.
            case SagaOutcome.Failed(reason) =>
              Logger[F].warn(s"saga '${saga.name}' instance ${record.id} failed: $reason")
            case _ => Async[F].unit
    yield ()

  /** Encode each command and stamp the saga's own headers over any the caller supplied. */
  private def encodeRequests[S, Req, Rep](
    saga: Saga[A, S, Req, Rep],
    id: UUID,
    step: Int,
    requests: List[SagaRequest[Req]],
  ): F[List[OutgoingMessage]] =
    requests.zipWithIndex.traverse { case (request, ordinal) =>
      saga.requestCodec.encode(request.payload) match
        case Left(error) =>
          Async[F].raiseError(
            new RuntimeException(s"saga '${saga.name}' failed to encode a request: ${error.getMessage}", error),
          )
        case Right(payload) =>
          Async[F].pure(
            OutgoingMessage(
              topic = request.topic,
              key = request.key,
              payload = payload,
              headers = request.headers ++ Map(
                SagaHeaders.Name           -> saga.name,
                SagaHeaders.Id             -> id.toString,
                SagaHeaders.IdempotencyKey -> SagaRequestRef.idempotencyKey(id, step, ordinal),
                SagaHeaders.ReplyTo        -> replyTopic,
              ),
            ),
          )
    }

  private def encodeState[S](saga: Saga[A, S, ?, ?], state: S): F[String] =
    saga.stateCodec.encode(state) match
      case Right(text) => Async[F].pure(text)
      case Left(error) =>
        Async[F].raiseError(
          new RuntimeException(s"saga '${saga.name}' failed to encode its state: ${error.getMessage}", error),
        )

  /** Consume `replyTopic` and apply every reply addressed to this saga, ignoring the rest.
    *
    * Needs no leader election — this is a consumer group, so running it on every instance is how it scales. But the
    * [[MessageSubscriber]] handed to this runner '''must not share a consumer group with another saga's runner''': the
    * broker would split the topic's partitions between them, and each would silently skip the replies that landed on it
    * for the other. Derive the group id per saga, e.g. with [[SagaRunner.replyGroupId]].
    */
  def replyLoop[S, Req, Rep](saga: Saga[A, S, Req, Rep]): Stream[F, Unit] =
    replies.subscribe(replyTopic, replyFromBeginning).evalMap { case (message, ack) =>
      handleReply(saga, message) *> ack
    }

  /** A reply is only ever acted on by the saga named on it, so anything else on the shared topic is acked untouched. */
  private def handleReply[S, Req, Rep](saga: Saga[A, S, Req, Rep], message: IncomingMessage): F[Unit] =
    if message.headers.get(SagaHeaders.Name).contains(saga.name) then routeReply(saga, message)
    else Async[F].unit

  private def routeReply[S, Req, Rep](saga: Saga[A, S, Req, Rep], message: IncomingMessage): F[Unit] =
    message.headers.get(SagaHeaders.Id).flatMap(parseUuid) match
      case None =>
        Logger[F].warn(s"saga '${saga.name}' received a reply with no usable ${SagaHeaders.Id} header, dropping it")
      case Some(id) =>
        repository.find(id).flatMap {
          case None =>
            // SagaRepository.start commits the instance row and its request in one transaction, so a reply cannot
            // legitimately outrun its row. Either the row was pruned or the correlation id did not come from us.
            Logger[F].warn(s"saga '${saga.name}' received a reply for unknown instance $id, dropping it")
          case Some(record) if record.status != SagaStatus.Pending =>
            Logger[F].debug(s"saga '${saga.name}' instance $id is ${record.status}, ignoring redelivered reply")
          case Some(record) =>
            applyReply(saga, record, message)
        }

  /** Decode failures are permanent: a redelivery would fail identically, so they are logged and acked rather than left
    * to block the partition. An instance whose reply is dropped this way stays pending and its deadline compensates it,
    * which is the fallback the timeout exists for. Every other failure propagates, leaving the message unacked so the
    * broker redelivers it.
    */
  private def applyReply[S, Req, Rep](
    saga: Saga[A, S, Req, Rep],
    record: SagaRecord,
    message: IncomingMessage,
  ): F[Unit] =
    (saga.stateCodec.decode(record.data), saga.replyCodec.decode(message.payload)) match
      case (Right(state), Right(reply)) =>
        val envelope = SagaReply(
          payload = reply,
          // Absent when the partner hand-rolled its reply instead of using `SagaHeaders.reply`, or echoed a key that is
          // not one of this instance's. A saga with a single request never needs it; a fan-out does.
          answering = message.headers.get(SagaHeaders.InReplyTo).flatMap(SagaRequestRef.parse(_, record.id)),
          headers = message.headers,
        )
        applyDecision(saga, record, saga.onReply(contextOf(saga.name, record), state, envelope))
      case (Left(error), _) =>
        Logger[F].error(error)(
          s"saga '${saga.name}' could not decode the stored state of instance ${record.id}; " +
            "leaving it pending for its deadline to compensate",
        )
      case (_, Left(error)) =>
        Logger[F].error(error)(s"saga '${saga.name}' could not decode a reply for instance ${record.id}, dropping it")

  private def contextOf(sagaName: String, record: SagaRecord): SagaContext =
    SagaContext(record.id, sagaName, record.key, record.step)

  private def parseUuid(value: String): Option[UUID] = Try(UUID.fromString(value)).toOption

  /** Claim instances whose deadline has passed and let the saga compensate them.
    *
    * Needs no leader election: [[SagaRepository.claimExpired]] leases each instance it hands over, so several instances
    * polling at once never compensate the same one twice.
    */
  def timerLoop[S, Req, Rep](saga: Saga[A, S, Req, Rep]): Stream[F, Unit] =
    Stream.awakeEvery[F](timerPollInterval).evalMap(_ => drainDue(saga))

  /** Keep claiming while passes come back full, so a backlog is not rationed to one batch per poll interval — but stop
    * after `maxDrainPasses`.
    *
    * Claiming only hides an instance for the repository's claim ttl, not for the length of this drain, so a drain that
    * outlasts that ttl can be handed an instance it already dealt with. Instances that keep failing to compensate stay
    * pending and would otherwise cycle here indefinitely, starving the poll tick. Whatever is left over waits for the
    * next one, which is what the poll interval is for.
    */
  private def drainDue[S, Req, Rep](saga: Saga[A, S, Req, Rep], passesLeft: Int = maxDrainPasses): F[Unit] =
    repository.claimExpired(saga.name, timerBatchSize)(handleTimeouts(saga, _)).flatMap { claimed =>
      if claimed >= timerBatchSize && passesLeft > 1 then drainDue(saga, passesLeft - 1)
      else Async[F].unit
    }

  /** Contain failures per instance: a claim hands over a whole batch, and letting one escape would abandon the rest of
    * it. A failed instance keeps its pushed-out deadline and comes back on a later claim.
    */
  private def handleTimeouts[S, Req, Rep](saga: Saga[A, S, Req, Rep], records: List[SagaRecord]): F[Unit] =
    records.traverse_ { record =>
      handleTimeout(saga, record).handleErrorWith { error =>
        Logger[F].error(error)(s"saga '${saga.name}' failed to compensate instance ${record.id} on timeout")
      }
    }

  private def handleTimeout[S, Req, Rep](saga: Saga[A, S, Req, Rep], record: SagaRecord): F[Unit] =
    saga.stateCodec.decode(record.data) match
      case Right(state) =>
        applyDecision(saga, record, saga.onTimeout(contextOf(saga.name, record), state))
      case Left(error) =>
        // Deliberately unlike the reply path, which leaves such an instance pending: there the deadline is still a
        // useful fallback, whereas here the deadline is what just fired. Left pending it would be re-claimed and
        // re-logged every claim ttl forever, so mark it failed to stop the churn and make it findable.
        Logger[F].error(error)(
          s"saga '${saga.name}' could not decode the stored state of instance ${record.id}, marking it failed",
        ) *> repository.advance(record.id, record.step, SagaStatus.Failed, record.step, record.data, None).void

  /** Run all three loops until cancelled, holding a [[LeaderElection]] lease for the trigger loop only.
    *
    * The lease is taken inside the stream, so a standby's reply and timer loops run from the start and only its trigger
    * loop waits — leadership changing hands disturbs nothing else.
    *
    * Like [[Projector.run]], the stream ends only on an unrecoverable error, and one loop failing takes the others down
    * with it; restarting is the caller's job.
    */
  def run[S, Req, Rep](saga: Saga[A, S, Req, Rep], leaderElection: LeaderElection[F]): Stream[F, Unit] =
    val trigger = Stream
      .eval(leaderElection.runAsLeader(loopName(saga.name))(triggerLoop(saga).compile.drain))
      // Losing the lease sends `runAsLeader` back to acquiring, so reaching here means the trigger loop itself stopped
      // without failing. Nothing would restart it, and this instance would go on serving replies while silently
      // starting no new sagas — so fail instead, and let the caller's supervisor bring the whole runner back.
      .flatMap(_ => Stream.raiseError[F](new IllegalStateException(s"saga '${saga.name}' trigger loop stopped")))
    trigger.merge(replyLoop(saga)).merge(timerLoop(saga))

object SagaRunner:

  /** Name of the trigger loop's checkpoint and of its leader-election lease. Prefixed so a saga cannot collide with a
    * projection that happens to share its name.
    */
  def loopName(sagaName: String): String = s"saga:$sagaName"

  /** Consumer group for a saga's reply loop. Each saga needs its own, or sagas sharing a reply topic steal each other's
    * replies — see [[SagaRunner.replyLoop]].
    */
  def replyGroupId(serviceGroupId: String, sagaName: String): String = s"$serviceGroupId.saga.$sagaName"

  def apply[F[_]: Async: Logger, A <: Event](
    store: EventStore[F, A] & EventNotification[F] & TransactionalMessages[F, A],
    checkpoint: ProjectionCheckpoint[F],
    repository: SagaRepository[F],
    replies: MessageSubscriber[F],
    replyTopic: String,
    batchSize: Int = 100,
    timerPollInterval: FiniteDuration = 5.seconds,
    timerBatchSize: Int = 64,
    maxDrainPasses: Int = 10,
    replyFromBeginning: Boolean = true,
  ): SagaRunner[F, A] =
    new SagaRunner[F, A](
      store, checkpoint, repository, replies, replyTopic, batchSize, timerPollInterval, timerBatchSize, maxDrainPasses,
      replyFromBeginning,
    )
