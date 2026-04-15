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

import cats.effect.*
import fs2.io.net.Network
import natchez.Trace.Implicits.noop
import pureconfig.ConfigSource
import skunk.*

import persistent4s.*
import persistent4s.circe.CirceEventCodec
import persistent4s.examples.library.domain.*
import persistent4s.examples.library.domain.book.{BookProjection, BookRepository}
import persistent4s.examples.library.domain.borrowing.{BorrowingProjection, BorrowingRepository}
import persistent4s.examples.library.domain.member.{MemberProjection, MemberRepository}
import persistent4s.postgres.{PostgresConfig, PostgresEventStore, PostgresModule, PostgresProjectionCheckpoint}

final class LibraryModule private (
  val store: PostgresEventStore[IO, LibraryEvent],
  val bookProjection: BookProjection[IO],
  val memberProjection: MemberProjection[IO],
  val borrowingProjection: BorrowingProjection[IO],
  val bookRepository: BookRepository[IO],
  val memberRepository: MemberRepository[IO],
  val borrowingRepository: BorrowingRepository[IO],
)

object LibraryModule:

  val eventCodec: EventCodec[LibraryEvent] = CirceEventCodec.make[LibraryEvent](
    encodeEvent = LibraryEvent.encoder.apply,
    decodeEvent = (eventType, json) => LibraryEvent.decoder(eventType.value, json).left.map(e => e: Throwable),
  )

  def make(configPath: String = "persistent4s.postgres"): Resource[IO, LibraryModule] =
    for
      store    <- PostgresModule.make[IO, LibraryEvent](eventCodec, configPath)
      config   <- Resource.eval(loadConfig(configPath))
      viewPool <- Session.pooled[IO](
                    host = config.host, port = config.port, user = config.user, password = Some(config.password),
                    database = config.database, max = config.maxConnections,
                  )
      bookRepo       = BookRepository.make[IO](viewPool)
      memberRepo     = MemberRepository.make[IO](viewPool)
      borrowingRepo  = BorrowingRepository.make[IO](viewPool)
      checkpoint     = PostgresProjectionCheckpoint.make[IO](viewPool)
      bookProj      <- Resource.eval(BookProjection.make[IO](bookRepo))
      memberProj    <- Resource.eval(MemberProjection.make[IO](memberRepo))
      borrowingProj <- Resource.eval(BorrowingProjection.make[IO](borrowingRepo))
      projector      = DefaultProjector[IO, LibraryEvent](store, checkpoint)
      _             <- projector.run(bookProj).compile.drain.background
      _             <- projector.run(memberProj).compile.drain.background
      _             <- projector.run(borrowingProj).compile.drain.background
    yield new LibraryModule(store, bookProj, memberProj, borrowingProj, bookRepo, memberRepo, borrowingRepo)

  private def loadConfig(configPath: String): IO[PostgresConfig] =
    IO.delay(ConfigSource.default.at(configPath).load[PostgresConfig]).flatMap {
      case Right(config) => IO.pure(config)
      case Left(errors)  => IO.raiseError(new RuntimeException(errors.prettyPrint()))
    }
