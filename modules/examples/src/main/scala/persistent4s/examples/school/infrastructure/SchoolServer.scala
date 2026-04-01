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

package persistent4s.examples.school.infrastructure

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.HttpRoutes
import smithy4s.http4s.SimpleRestJsonBuilder
import smithy4s.http4s.swagger.docs

import persistent4s.examples.school.api.*
import persistent4s.examples.school.application.*

object SchoolServer extends IOApp.Simple:

  def run: IO[Unit] =
    SchoolModule.make.use { module =>
      val routes: Resource[IO, HttpRoutes[IO]] =
        for
          studentRoutes    <- SimpleRestJsonBuilder.routes(StudentServiceImpl(module)).resource
          courseRoutes     <- SimpleRestJsonBuilder.routes(CourseServiceImpl(module)).resource
          enrollmentRoutes <- SimpleRestJsonBuilder.routes(EnrollmentServiceImpl(module)).resource
          eventsRoutes     <- SimpleRestJsonBuilder.routes(EventsServiceImpl(module)).resource
          docsRoutes        = docs[IO](StudentService, CourseService, EnrollmentService, EventsService)
        yield studentRoutes <+> courseRoutes <+> enrollmentRoutes <+> eventsRoutes <+> docsRoutes

      routes.use { r =>
        EmberServerBuilder
          .default[IO]
          .withHost(host"0.0.0.0")
          .withPort(port"8181")
          .withHttpApp(r.orNotFound)
          .build
          .useForever
      }
    }
