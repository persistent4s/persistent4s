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

import java.time.Instant

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger
import weaver.SimpleIOSuite

/** Unit tests for [[SagaParticipant]] — the answering side's plumbing.
  *
  * No broker and no store: the participant's whole job happens between receiving a message and acknowledging it, so a
  * fake subscriber emitting a fixed list is enough to see all of it. Nearly every test asserts the pair "was it
  * handled" and "was it acked", because those two together are what decide whether a message is gone for good or comes
  * back.
  */
object SagaParticipantSuite extends SimpleIOSuite:

  private val Topic = "inventory.commands"

  final case class Reserve(orderId: String)

  final case class Release(orderId: String)

  final case class Answer(accepted: Boolean)

  /** The names these requests travel under. Declared beside the types exactly as a contract module would, because that
    * single declaration is now what both the sender's header and the participant's routing table are read from.
    */
  private given RequestType[Reserve] = RequestType("reserve")

  private given RequestType[Release] = RequestType("release")

  /** Payloads are bare strings rather than JSON: what is under test is routing and addressing, and a real codec here
    * would only add a way for these tests to fail for reasons that are not the participant's.
    */
  private given MessageCodec[Reserve] with

    def encode(message: Reserve): Either[Throwable, String] = Right(message.orderId)

    def decode(payload: String): Either[Throwable, Reserve] =
      if payload.startsWith("o-") then Right(Reserve(payload))
      else Left(new RuntimeException(s"not an order id: $payload"))

  private given MessageDecoder[Release] with

    def decode(payload: String): Either[Throwable, Release] = Right(Release(payload))

  private given MessageEncoder[Answer] with

    def encode(message: Answer): Either[Throwable, String] = Right(if message.accepted then "yes" else "no")

  /** An encoder that cannot: the only way to reach the "reply could not be serialized" path. */
  private val unencodable: MessageEncoder[Answer] =
    new MessageEncoder[Answer]:
      def encode(message: Answer): Either[Throwable, String] = Left(new RuntimeException("boom"))

  // ----- fakes -----

  final class FakeSubscriber(messages: List[IncomingMessage], acked: Ref[IO, List[IncomingMessage]])
      extends MessageSubscriber[IO]:

    def subscribe(topic: String, fromBeginning: Boolean): Stream[IO, (IncomingMessage, IO[Unit])] =
      Stream.emits(messages).map(message => (message, acked.update(_ :+ message)))

  final class FakePublisher(published: Ref[IO, List[OutgoingMessage]]) extends MessagePublisher[IO]:

    def publish(message: OutgoingMessage): IO[Unit] = published.update(_ :+ message)

    def publish(messages: List[OutgoingMessage]): IO[Unit] = published.update(_ ++ messages)

  /** Collects what the participant logs. Every path that drops a message logs and does nothing else, so for those the
    * log line ''is'' the behaviour and asserting on it is not incidental.
    */
  final class CapturingLogger(entries: Ref[IO, List[String]]) extends Logger[IO]:

    private def record(level: String, message: String): IO[Unit] = entries.update(_ :+ s"$level $message")

    def error(message: => String): IO[Unit] = record("ERROR", message)

    def warn(message: => String): IO[Unit] = record("WARN", message)

    def info(message: => String): IO[Unit] = record("INFO", message)

    def debug(message: => String): IO[Unit] = record("DEBUG", message)

    def trace(message: => String): IO[Unit] = record("TRACE", message)

    def error(t: Throwable)(message: => String): IO[Unit] = record("ERROR", s"$message [${t.getMessage}]")

    def warn(t: Throwable)(message: => String): IO[Unit] = record("WARN", s"$message [${t.getMessage}]")

    def info(t: Throwable)(message: => String): IO[Unit] = record("INFO", s"$message [${t.getMessage}]")

    def debug(t: Throwable)(message: => String): IO[Unit] = record("DEBUG", s"$message [${t.getMessage}]")

    def trace(t: Throwable)(message: => String): IO[Unit] = record("TRACE", s"$message [${t.getMessage}]")

  extension (logged: List[String])

    /** Anchored on an identifier and a severity rather than on wording, so improving a sentence is not a failure. */
    def reported(level: String, subject: String): Boolean =
      logged.exists(entry => entry.startsWith(level) && entry.contains(subject))

  // ----- messages -----

  private val instance = "3f2a1c00-0000-0000-0000-000000000001"

  /** A request as the runner would have stamped it: addressed, and naming the request it wants answered. */
  private def request(requestType: String, payload: String): IncomingMessage =
    IncomingMessage(
      topic = Topic,
      key = Some("o-1"),
      payload = payload,
      headers = Map(
        SagaHeaders.RequestType    -> requestType,
        SagaHeaders.Name           -> "reserve-stock",
        SagaHeaders.Id             -> instance,
        SagaHeaders.ReplyTo        -> "orders.replies",
        SagaHeaders.IdempotencyKey -> s"$instance:0:0:stock",
      ),
    )

  private def reserve(orderId: String = "o-1"): IncomingMessage = request(requestType = "reserve", payload = orderId)

  /** A request from something that is not a saga: it says what it is, and nothing about where to answer. */
  private def unaddressed: IncomingMessage = reserve().copy(headers = Map(SagaHeaders.RequestType -> "reserve"))

  // ----- driving -----

  final case class Run(acked: List[IncomingMessage], published: List[OutgoingMessage], logged: List[String])

  /** Run `build`'s participant over `messages` and report everything observable about the pass. */
  private def drive(messages: IncomingMessage*)(
    build: (Logger[IO], MessagePublisher[IO]) => SagaParticipant[IO],
  ): IO[Run] =
    for
      acked     <- Ref.of[IO, List[IncomingMessage]](Nil)
      published <- Ref.of[IO, List[OutgoingMessage]](Nil)
      entries   <- Ref.of[IO, List[String]](Nil)
      _         <- build(CapturingLogger(entries), FakePublisher(published))
             .subscribe(FakeSubscriber(messages.toList, acked), Topic)
             .compile
             .drain
      wasAcked <- acked.get
      wasSent  <- published.get
      logged   <- entries.get
    yield Run(wasAcked, wasSent, logged)

  // ----- routing -----

  test("a request is decoded and handed to the handler registered for its type") {
    for
      decoded <- Ref.of[IO, List[Reserve]](Nil)
      run     <- drive(reserve("o-7")) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO].on[Reserve]((_, command) => decoded.update(_ :+ command))
             }
      commands <- decoded.get
    yield expect.all(commands == List(Reserve("o-7")), run.acked.size == 1)
  }

  test("each request type goes to its own handler and no other") {
    for
      seen <- Ref.of[IO, List[String]](Nil)
      run  <- drive(reserve(), request(requestType = "release", payload = "o-2")) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO]
                 .on[Reserve]((_, c) => seen.update(_ :+ s"reserve:${c.orderId}"))
                 .on[Release]((_, c) => seen.update(_ :+ s"release:${c.orderId}"))
             }
      routed <- seen.get
    yield expect.all(routed == List("reserve:o-1", "release:o-2"), run.acked.size == 2)
  }

  test("the handler is given the moment the request was picked up") {
    // Read once, by the participant, and passed as data — which is what lets a handler judge staleness without a clock
    // of its own, and so stay a pure function.
    val before = Instant.now().minusSeconds(1)
    for
      contexts <- Ref.of[IO, List[RequestContext]](Nil)
      _        <- drive(reserve()) { (log, _) =>
             given Logger[IO] = log
             SagaParticipant[IO].on[Reserve]((ctx, _) => contexts.update(_ :+ ctx))
           }
      seen <- contexts.get
    yield expect.all(seen.size == 1, seen.forall(_.receivedAt.isAfter(before)))
  }

  // ----- messages it refuses to route -----

  test("a request of an unknown type is dropped, acknowledged and reported") {
    // Acked deliberately: a redelivery would be just as unroutable, so leaving it unacked would block the partition
    // forever on a message nothing can ever handle.
    for
      seen <- Ref.of[IO, List[Reserve]](Nil)
      run  <- drive(request(requestType = "teleport", payload = "o-1")) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO].on[Reserve]((_, c) => seen.update(_ :+ c))
             }
      handled <- seen.get
    yield expect.all(handled.isEmpty, run.acked.size == 1, run.logged.reported("ERROR", "teleport"))
  }

  test("a request with no request-type header at all is dropped, acknowledged and reported") {
    for
      seen <- Ref.of[IO, List[Reserve]](Nil)
      run  <- drive(reserve().copy(headers = reserve().headers - SagaHeaders.RequestType)) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO].on[Reserve]((_, c) => seen.update(_ :+ c))
             }
      handled <- seen.get
    yield expect.all(handled.isEmpty, run.acked.size == 1, run.logged.reported("ERROR", SagaHeaders.RequestType))
  }

  test("an undecodable payload is dropped and acknowledged, and the message behind it is still handled") {
    // The one that matters on a shared topic: one poisonous record must not cost every record queued behind it.
    for
      seen <- Ref.of[IO, List[Reserve]](Nil)
      run  <- drive(request(requestType = "reserve", payload = "not an order id"), reserve("o-9")) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO].on[Reserve]((_, c) => seen.update(_ :+ c))
             }
      handled <- seen.get
    yield expect.all(handled == List(Reserve("o-9")), run.acked.size == 2, run.logged.reported("ERROR", "reserve"))
  }

  test("a handler that raises leaves its message unacknowledged, so the broker will redeliver it") {
    // The other half of the ack policy: a failure a redelivery could plausibly fix must not be swallowed.
    for
      acked   <- Ref.of[IO, List[IncomingMessage]](Nil)
      entries <- Ref.of[IO, List[String]](Nil)
      outcome <- {
        given Logger[IO] = CapturingLogger(entries)
        SagaParticipant[IO]
          .on[Reserve]((_, _) => IO.raiseError(new RuntimeException("store is down")))
          .subscribe(FakeSubscriber(List(reserve()), acked), Topic)
          .compile
          .drain
          .attempt
      }
      wasAcked <- acked.get
    yield expect.all(outcome.isLeft, wasAcked.isEmpty)
  }

  // ----- addressing -----

  test("a request that nominates nowhere to answer is still handled, and reported") {
    // Legitimate — it just means nobody is waiting. Handling it is right; doing so silently is not, because a saga
    // whose requests arrive unaddressed would otherwise be indistinguishable from a partner that never answers.
    for
      contexts <- Ref.of[IO, List[RequestContext]](Nil)
      run      <- drive(unaddressed) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO].on[Reserve]((ctx, _) => contexts.update(_ :+ ctx))
             }
      seen <- contexts.get
    yield expect.all(
      seen.size == 1,
      seen.forall(!_.isAddressed),
      run.acked.size == 1,
      run.logged.reported("WARN", "reserve"),
    )
  }

  // ----- registration mistakes -----

  test("a second handler for one request type never runs, and the collision is reported") {
    for
      seen <- Ref.of[IO, List[String]](Nil)
      run  <- drive(reserve()) { (log, _) =>
               given Logger[IO] = log
               SagaParticipant[IO]
                 .on[Reserve]((_, _) => seen.update(_ :+ "first"))
                 .on[Reserve]((_, _) => seen.update(_ :+ "second"))
             }
      routed <- seen.get
    yield expect.all(routed == List("first"), run.logged.reported("WARN", "reserve"))
  }

  test("a participant with no handlers reports that it will drop everything") {
    for run <- drive(reserve()) { (log, _) =>
                 given Logger[IO] = log
                 SagaParticipant[IO]
               }
    yield expect.all(run.acked.size == 1, run.logged.reported("WARN", Topic))
  }

  // ----- replying -----

  test("replying publishes the handler's answer, encoded and addressed back to the caller") {
    for run <- drive(reserve()) { (log, publisher) =>
                 given Logger[IO] = log
                 SagaParticipant[IO]
                   .replying[Reserve, Answer](publisher)((_, _) => IO.pure(Answer(accepted = true)))
               }
    yield expect.all(
      run.acked.size == 1,
      run.published.size == 1,
      run.published.head.topic == "orders.replies",
      run.published.head.payload == "yes",
      // The correlation headers the runner routes on, none of which the handler had to know existed.
      run.published.head.headers.get(SagaHeaders.Name) == Some("reserve-stock"),
      run.published.head.headers.get(SagaHeaders.Id) == Some(instance),
      run.published.head.headers.get(SagaHeaders.InReplyTo) == Some(s"$instance:0:0:stock"),
      // Keyed by the instance, not by anything the partner chose: two replies to one instance on one partition are
      // handled one at a time, and of two concurrent decisions only one would survive the step guard.
      run.published.head.key == Some(instance),
    )
  }

  test("replying to a request that nominates nowhere to answer publishes nothing") {
    for run <- drive(unaddressed) { (log, publisher) =>
                 given Logger[IO] = log
                 SagaParticipant[IO]
                   .replying[Reserve, Answer](publisher)((_, _) => IO.pure(Answer(accepted = true)))
               }
    yield expect.all(run.acked.size == 1, run.published.isEmpty)
  }

  test("a reply that cannot be encoded is reported and dropped, without failing the stream") {
    // Nothing was written, so there is nothing to undo. Blocking the partition on a failure that will repeat
    // identically costs more than letting the asking saga reach its deadline — the opposite of the transactional case,
    // where failing to reply has to undo the append as well.
    for run <- drive(reserve()) { (log, publisher) =>
                 given Logger[IO] = log
                 given MessageEncoder[Answer] = unencodable
                 SagaParticipant[IO]
                   .replying[Reserve, Answer](publisher)((_, _) => IO.pure(Answer(accepted = true)))
               }
    yield expect.all(run.acked.size == 1, run.published.isEmpty, run.logged.reported("ERROR", "boom"))
  }
