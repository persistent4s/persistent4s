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

package persistent4s.examples.courses.catalog.infrastructure

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.http4s.HttpRoutes
import smithy4s.http4s.SimpleRestJsonBuilder
import smithy4s.http4s.swagger.docs

import persistent4s.EventStore
import persistent4s.examples.courses.catalog.api.*
import persistent4s.examples.courses.catalog.application.*
import persistent4s.examples.courses.catalog.domain.CatalogEvent

object CatalogRoutes:

  def make(module: CatalogModule): Resource[IO, HttpRoutes[IO]] =
    given EventStore[IO, CatalogEvent] = module.store

    for
      courseRoutes <- SimpleRestJsonBuilder.routes(CourseServiceImpl(module.courseRepository)).resource
      eventsRoutes <- SimpleRestJsonBuilder.routes(EventsServiceImpl(module.store)).resource
      docsRoutes    = docs[IO](CourseService, EventsService)
    yield courseRoutes <+> eventsRoutes <+> docsRoutes
