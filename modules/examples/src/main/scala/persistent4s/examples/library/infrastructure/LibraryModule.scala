/*
 * Copyright 2026 persistent4s
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

import cats.effect.*
import fs2.Stream
import fs2.io.net.Network
import natchez.Trace.Implicits.noop

import persistent4s.*
import persistent4s.circe.CirceEventCodec
import persistent4s.examples.library.domain.*
import persistent4s.examples.library.domain.book.BookProjection
import persistent4s.examples.library.domain.borrowing.BorrowingProjection
import persistent4s.examples.library.domain.member.MemberProjection
import persistent4s.postgres.{PostgresEventStore, PostgresModule}
import persistent4s.testkit.InMemoryProjectionCheckpoint

final class LibraryModule private (
  val store: PostgresEventStore[IO, LibraryEvent],
  val bookProjection: BookProjection[IO],
  val memberProjection: MemberProjection[IO],
  val borrowingProjection: BorrowingProjection[IO],
)

object LibraryModule:

  val eventCodec: EventCodec[LibraryEvent] = CirceEventCodec.make[LibraryEvent](
    encodeEvent = LibraryEvent.encoder.apply,
    decodeEvent = (eventType, json) => LibraryEvent.decoder(eventType, json).left.map(e => e: Throwable),
  )

  def make(configPath: String = "persistent4s.postgres"): Resource[IO, LibraryModule] =
    for
      store         <- PostgresModule.make[IO, LibraryEvent](eventCodec, configPath)
      checkpoint    <- Resource.eval(InMemoryProjectionCheckpoint.make[IO])
      bookProj      <- Resource.eval(BookProjection.make[IO])
      memberProj    <- Resource.eval(MemberProjection.make[IO])
      borrowingProj <- Resource.eval(BorrowingProjection.make[IO])
      _             <- runProjector(store, checkpoint, bookProj).compile.drain.background
      _             <- runProjector(store, checkpoint, memberProj).compile.drain.background
      _             <- runProjector(store, checkpoint, borrowingProj).compile.drain.background
    yield new LibraryModule(store, bookProj, memberProj, borrowingProj)

  private def runProjector[A](
    store: PostgresEventStore[IO, A] & EventNotification[IO],
    checkpoint: ProjectionCheckpoint[IO],
    projection: Projection[IO, A],
  ): Stream[IO, Unit] =
    val processEvents: Stream[IO, Unit] =
      Stream
        .eval(checkpoint.load(projection.name))
        .flatMap { lastPosition =>
          store.readFrom(lastPosition.getOrElse(-1L), projection.filter)
        }
        .evalMap { envelope =>
          projection.handle(envelope) *>
            checkpoint.save(projection.name, envelope.metadata.globalPosition)
        }

    (Stream.emit(()) ++ store.notification).flatMap(_ => processEvents)
