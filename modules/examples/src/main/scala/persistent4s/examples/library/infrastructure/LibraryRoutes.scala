/*
 * Copyright 2026 Bastien Jolidon
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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.http4s.HttpRoutes
import smithy4s.http4s.SimpleRestJsonBuilder
import smithy4s.http4s.swagger.docs

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.application.*
import persistent4s.examples.library.domain.LibraryEvent

object LibraryRoutes:

  def make(module: LibraryModule): Resource[IO, HttpRoutes[IO]] =
    given EventStore[IO, LibraryEvent] = module.store

    for
      bookRoutes <- SimpleRestJsonBuilder
                      .routes(BookServiceImpl(module.bookRepository))
                      .resource
      memberRoutes <- SimpleRestJsonBuilder
                        .routes(MemberServiceImpl(module.memberRepository))
                        .resource
      borrowingRoutes <- SimpleRestJsonBuilder
                           .routes(BorrowingServiceImpl(module.borrowingRepository))
                           .resource
      eventsRoutes <- SimpleRestJsonBuilder
                        .routes(EventsServiceImpl(module))
                        .resource
      docsRoutes = docs[IO](
                     BookService,
                     MemberService,
                     BorrowingService,
                     EventsService,
                   )
    yield bookRoutes <+> memberRoutes <+> borrowingRoutes <+> eventsRoutes <+> docsRoutes
