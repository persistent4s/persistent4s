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

package persistent4s.examples.library.infrastructure

import java.util.UUID
import scala.concurrent.duration.*
import cats.effect.*

import fs2.concurrent.Topic
import fs2.io.net.Network

import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

import persistent4s.*
import persistent4s.examples.library.api.ValidationError
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.book.{AddBook, BookProjection, BookRepository, BookState}
import persistent4s.examples.library.domain.borrowing.{BorrowingProjection, BorrowingRepository}
import persistent4s.examples.library.domain.member.{MemberProjection, MemberRepository}
import persistent4s.postgres.PostgresModule
import persistent4s.monitoring.MonitoringServer

final class LibraryModule private (
  val store: EventStore[IO, LibraryEvent] & EventNotification[IO],
  val commands: CommandRuntime[IO, LibraryEvent],
  val projections: RunningProjections[IO],
  val bookRepository: BookRepository,
  val memberRepository: MemberRepository,
  val borrowingRepository: BorrowingRepository,
  val addBookSyncHandler: SyncCommandHandler[IO, AddBook, LibraryEvent, UUID, BookState],
)

object LibraryModule:

  def make(configPath: String = "persistent4s.postgres"): Resource[IO, LibraryModule] =
    for
      resources    <- PostgresModule.make[IO, LibraryEvent](LibraryEvent.eventCodec, configPath)
      store         = resources.eventStore
      commands      = resources.commandRuntime
      checkpoint    = resources.checkpoint
      _            <- MonitoringServer.make(checkpoint, resources.sendNotification)
      bookRepo      = BookRepository.make(resources.sessions)
      memberRepo    = MemberRepository.make(resources.sessions)
      borrowingRepo = BorrowingRepository.make(resources.sessions)
      bookProj      = BookProjection(bookRepo)
      // AddBook answers with the projected book, so its projection publishes to a topic the command waits on.
      bookTopic   <- Resource.eval(Topic[IO, (UUID, Either[Throwable, Map[UUID, Option[BookState]]])])
      projections <- resources.projectionRuntime.startAll { registered =>
                       registered.run(bookProj, Some(bookTopic))
                       registered.run(MemberProjection(memberRepo))
                       registered.run(BorrowingProjection(borrowingRepo))
                     }
      addBookSyncHandler = SyncCommandHandler.fromEventSourced(
                             AddBook.Handler,
                             commands,
                             { case AddBook.Error.AlreadyExists(id) =>
                               ValidationError(s"Book already exists: $id")
                             },
                             bookTopic,
                             bookProj.filter,
                             timeout = 5.seconds,
                           )
    yield new LibraryModule(store, commands, projections, bookRepo, memberRepo, borrowingRepo, addBookSyncHandler)
