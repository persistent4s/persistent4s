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
import java.util.UUID

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import weaver.SimpleIOSuite

object EventSourcedCommandHandlerMessagesSuite extends SimpleIOSuite:

  final case class Reserve(id: String, amount: Int)

  sealed trait StockEvent extends Event

  final case class Reserved(id: String, amount: Int) extends StockEvent

  enum StockRejection:

    case Exhausted

  private val ItemScope = Scope[String]("test.item")

  private given EventSchema[Reserved] = EventSchema[Reserved]("test.reserved").scopedBy(ItemScope)(_.id)

  private given MessageEncoder[String] with

    def encode(message: String): Either[Throwable, String] = Right(message)

  /** Reserves up to 10 units per item; asking again for what is already reserved writes nothing. */
  private object ReserveHandler extends EventSourcedCommandHandler[Reserve, Int, StockEvent, StockRejection]:

    protected val behavior = handler(0):
      scope(ItemScope)(_.id)

      on[Reserved].matching(_.id, _.id).evolve((reserved, event) => reserved + event.amount)

      reject:
        case (reserved, command) if reserved != command.amount && reserved + command.amount > 10 =>
          StockRejection.Exhausted

      emitMany((reserved, command) =>
        if reserved == command.amount then Nil else List(Reserved(command.id, command.amount)),
      )

  /** Records what the plain append and the dual-write append receive, and can lose the first dual write to a conflict. */
  final private class MessagingStore(
    events: Ref[IO, Vector[EventEnvelope[StockEvent]]],
    messages: Ref[IO, Vector[OutgoingMessage]],
    plainAppends: Ref[IO, Int],
    conflicts: Ref[IO, Int],
  ) extends EventStore[IO, StockEvent]
      with TransactionalMessages[IO, StockEvent]:

    def history: IO[Vector[EventEnvelope[StockEvent]]] = events.get

    def enqueued: IO[Vector[OutgoingMessage]] = messages.get

    def plainAppendCount: IO[Int] = plainAppends.get

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      pending: List[PendingEvent[StockEvent]]*,
    ): IO[List[EventEnvelope[StockEvent]]] =
      plainAppends.update(_ + 1) *> write(pending.toList.flatten)

    def appendUnchecked(pending: List[PendingEvent[StockEvent]]*): IO[List[EventEnvelope[StockEvent]]] =
      write(pending.toList.flatten)

    def appendWithMessages(
      eventFilter: EventFilter,
      expectedIndex: Long,
      outgoing: List[OutgoingMessage],
      pending: List[PendingEvent[StockEvent]]*,
    ): IO[List[EventEnvelope[StockEvent]]] =
      val flattened = pending.toList.flatten
      conflicts.modify(left => (left - 1, left)).flatMap { left =>
        if flattened.nonEmpty && left > 0 then
          IO.raiseError(new IndexConflictException(expectedIndex, expectedIndex + 1))
        else write(flattened) <* messages.update(_ ++ outgoing)
      }

    def appendUncheckedWithMessages(
      outgoing: List[OutgoingMessage],
      pending: List[PendingEvent[StockEvent]]*,
    ): IO[List[EventEnvelope[StockEvent]]] =
      write(pending.toList.flatten) <* messages.update(_ ++ outgoing)

    def currentRevision(eventFilter: EventFilter): IO[Long] =
      events.get.map(_.lastOption.fold(0L)(_.metadata.globalPosition))

    def readForProjection(after: ReadCursor, eventFilter: EventFilter, maxEvents: Int): IO[ProjectionRead[StockEvent]] =
      ProjectionRead.byPosition(readFrom(after.globalPosition, eventFilter, Some(maxEvents)))

    def cursorAt(globalPosition: Long): IO[ReadCursor] =
      IO.pure(if globalPosition <= 0L then ReadCursor.Start else ReadCursor(0L, globalPosition))

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int],
    ): Stream[IO, EventEnvelope[StockEvent]] =
      Stream.evalSeq(events.get.map(_.filter(_.metadata.tags.exists(eventFilter.tags.contains))))

    private def write(pending: List[PendingEvent[StockEvent]]): IO[List[EventEnvelope[StockEvent]]] =
      events.modify { current =>
        val written = pending.zipWithIndex.map { (event, offset) =>
          EventEnvelope(
            EventMetadata(
              current.size.toLong + offset + 1L,
              UUID.randomUUID(),
              event.tags,
              event.eventType,
              event.isExternal,
              Instant.EPOCH,
              event.headers,
            ),
            event.payload,
          )
        }
        (current ++ written, written)
      }

  private def withStore[A](conflicts: Int = 0)(
    use: MessagingStore => (CommandRuntime[IO, StockEvent], TransactionalMessages[IO, StockEvent]) ?=> IO[A],
  ): IO[A] =
    (
      Ref.of[IO, Vector[EventEnvelope[StockEvent]]](Vector.empty),
      Ref.of[IO, Vector[OutgoingMessage]](Vector.empty),
      Ref.of[IO, Int](0),
      Ref.of[IO, Int](conflicts),
    ).mapN(MessagingStore(_, _, _, _)).flatMap { store =>
      given CommandRuntime[IO, StockEvent] = CommandRuntime.eventStoreOnly(store)
      given TransactionalMessages[IO, StockEvent] = store
      use(store)
    }

  private def note(outcome: Either[StockRejection, List[StockEvent]]): String =
    outcome.fold(rejection => s"rejected:$rejection", events => s"accepted:${events.size}")

  private def recorded(state: Int, command: Reserve, events: List[StockEvent]) =
    Right(List(OutgoingMessage("notes", Some(command.id), s"$state:${note(Right(events))}")))

  private def request(headers: Map[String, String]): RequestContext =
    RequestContext(IncomingMessage("stock.requests", Some("a"), "{}", headers), Instant.EPOCH)

  private val addressed = Map(
    SagaHeaders.ReplyTo        -> "orders.replies",
    SagaHeaders.Name           -> "order-saga",
    SagaHeaders.Id             -> "instance-1",
    SagaHeaders.IdempotencyKey -> "order-1:0:0",
  )

  test("an accepted command appends its events and its messages through the transactional store") {
    withStore() { store =>
      for
        result <- ReserveHandler.runWithMessages[IO](Reserve("a", 3))(recorded)
        events <- store.history
        sent   <- store.enqueued
        plain  <- store.plainAppendCount
      yield expect(result.map(_.map(_.payload)) == Right(List(Reserved("a", 3)))) and
        expect(events.map(_.payload) == Vector(Reserved("a", 3))) and
        expect(sent == Vector(OutgoingMessage("notes", Some("a"), "0:accepted:1"))) and
        expect(plain == 0)
    }
  }

  test("a rejection enqueues none of the command's messages and is returned") {
    withStore() { store =>
      for
        _      <- ReserveHandler.runWithMessages[IO](Reserve("a", 8))(recorded)
        result <- ReserveHandler.runWithMessages[IO](Reserve("a", 5))(recorded)
        events <- store.history
        sent   <- store.enqueued
      yield expect(result == Left(StockRejection.Exhausted)) and
        expect(events.map(_.payload) == Vector(Reserved("a", 8))) and
        expect(sent.map(_.payload) == Vector("0:accepted:1"))
    }
  }

  test("a command that decides nothing still enqueues its messages, with no events") {
    withStore() { store =>
      for
        _      <- ReserveHandler.runWithMessages[IO](Reserve("a", 3))(recorded)
        result <- ReserveHandler.runWithMessages[IO](Reserve("a", 3))(recorded)
        events <- store.history
        sent   <- store.enqueued
      yield expect(result.map(_.size) == Right(0)) and
        expect(events.size == 1) and
        expect(sent.map(_.payload) == Vector("0:accepted:1", "3:accepted:0"))
    }
  }

  test("messages that cannot be built abort the command without appending anything") {
    val failure = new IllegalStateException("cannot encode")
    withStore() { store =>
      for
        result <- ReserveHandler.runWithMessages[IO](Reserve("a", 3))((_, _, _) => Left(failure)).attempt
        events <- store.history
        sent   <- store.enqueued
      yield expect(result == Left(failure)) and expect(events.isEmpty) and expect(sent.isEmpty)
    }
  }

  test("a conflict retries the command and enqueues the messages of the attempt that commits only") {
    withStore(conflicts = 1) { store =>
      for
        result <- ReserveHandler.runWithMessages[IO](Reserve("a", 3))(recorded)
        events <- store.history
        sent   <- store.enqueued
      yield expect(result.map(_.size) == Right(1)) and
        expect(events.size == 1) and
        expect(sent.map(_.payload) == Vector("0:accepted:1"))
    }
  }

  test("runAnswering addresses the reply from the request, on acceptance and on rejection") {
    val answer = (_: Int, _: Reserve, outcome: Either[StockRejection, List[StockEvent]]) =>
      Some(PendingReply(note(outcome), Map("extra" -> "kept")))
    withStore() { store =>
      for
        accepted <- ReserveHandler.runAnswering[IO](request(addressed), Reserve("a", 8))(answer)
        rejected <- ReserveHandler.runAnswering[IO](request(addressed), Reserve("a", 5))(answer)
        sent     <- store.enqueued
      yield
        val expectedHeaders = Map(
          "extra"               -> "kept",
          SagaHeaders.Name      -> "order-saga",
          SagaHeaders.Id        -> "instance-1",
          SagaHeaders.InReplyTo -> "order-1:0:0",
        )
        expect(accepted.map(_.size) == Right(1)) and
          expect(rejected == Left(StockRejection.Exhausted)) and
          expect(
            sent == Vector(
              OutgoingMessage("orders.replies", Some("instance-1"), "accepted:1", expectedHeaders),
              OutgoingMessage("orders.replies", Some("instance-1"), "rejected:Exhausted", expectedHeaders),
            ),
          )
    }
  }

  test("runAnswering sends nothing for an unaddressed request or when there is nothing to answer") {
    withStore() { store =>
      for
        first <-
          ReserveHandler.runAnswering[IO](request(Map.empty), Reserve("a", 3))((_, _, _) => Some(PendingReply("ok")))
        second <- ReserveHandler.runAnswering[IO](request(addressed), Reserve("b", 3))((_, _, _) => None)
        events <- store.history
        sent   <- store.enqueued
      yield expect(first.isRight && second.isRight) and expect(events.size == 2) and expect(sent.isEmpty)
    }
  }
