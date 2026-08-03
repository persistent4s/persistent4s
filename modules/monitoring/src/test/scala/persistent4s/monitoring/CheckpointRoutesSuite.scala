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

package persistent4s.monitoring

import cats.effect.{IO, Ref}

import persistent4s.{EventStoreNotification, ProjectionCheckpointState}

import org.http4s.*
import org.http4s.implicits.*
import weaver.SimpleIOSuite

object CheckpointRoutesSuite extends SimpleIOSuite:

  private val proj = ProjectionCheckpointState("books", 42L, true, None)

  private def makeApp(
    states: List[ProjectionCheckpointState],
    captured: Ref[IO, List[EventStoreNotification]],
  ): HttpApp[IO] =
    new CheckpointRoutes[IO](IO.pure(states), n => captured.update(_ :+ n)).routes.orNotFound

  test("GET / returns 200") {
    for
      ref  <- Ref.of[IO, List[EventStoreNotification]](Nil)
      resp <- makeApp(List(proj), ref).run(Request[IO](Method.GET, uri"/"))
    yield expect(resp.status == Status.Ok)
  }

  test("GET / response is text/html") {
    for
      ref  <- Ref.of[IO, List[EventStoreNotification]](Nil)
      resp <- makeApp(List(proj), ref).run(Request[IO](Method.GET, uri"/"))
    yield expect(resp.contentType.exists(_.mediaType == MediaType.text.html))
  }

  test("GET / body contains projection name") {
    for
      ref  <- Ref.of[IO, List[EventStoreNotification]](Nil)
      resp <- makeApp(List(proj), ref).run(Request[IO](Method.GET, uri"/"))
      body <- resp.as[String]
    yield expect(body.contains("books"))
  }

  test("POST /checkpoints/{name}/pause sends PauseProjection and redirects") {
    for
      ref    <- Ref.of[IO, List[EventStoreNotification]](Nil)
      resp   <- makeApp(Nil, ref).run(Request[IO](Method.POST, uri"/checkpoints/books/pause"))
      notifs <- ref.get
    yield expect.all(
      resp.status == Status.SeeOther,
      notifs == List(EventStoreNotification.PauseProjection("books")),
    )
  }

  test("POST /checkpoints/{name}/resume sends ResumeProjection and redirects") {
    for
      ref    <- Ref.of[IO, List[EventStoreNotification]](Nil)
      resp   <- makeApp(Nil, ref).run(Request[IO](Method.POST, uri"/checkpoints/books/resume"))
      notifs <- ref.get
    yield expect.all(
      resp.status == Status.SeeOther,
      notifs == List(EventStoreNotification.ResumeProjection("books")),
    )
  }

  test("POST /checkpoints/{name}/index with valid index sends UpdateCheckpointIndex and redirects") {
    for
      ref    <- Ref.of[IO, List[EventStoreNotification]](Nil)
      body    = UrlForm("index" -> "99")
      req     = Request[IO](Method.POST, uri"/checkpoints/books/index").withEntity(body)
      resp   <- makeApp(Nil, ref).run(req)
      notifs <- ref.get
    yield expect.all(
      resp.status == Status.SeeOther,
      notifs == List(EventStoreNotification.UpdateCheckpointIndex("books", 99L)),
    )
  }

  test("POST /checkpoints/{name}/index with non-numeric index returns 400") {
    for
      ref  <- Ref.of[IO, List[EventStoreNotification]](Nil)
      body  = UrlForm("index" -> "not-a-number")
      req   = Request[IO](Method.POST, uri"/checkpoints/books/index").withEntity(body)
      resp <- makeApp(Nil, ref).run(req)
    yield expect(resp.status == Status.BadRequest)
  }

  test("POST /checkpoints/{name}/index with missing index returns 400") {
    for
      ref  <- Ref.of[IO, List[EventStoreNotification]](Nil)
      body  = UrlForm()
      req   = Request[IO](Method.POST, uri"/checkpoints/books/index").withEntity(body)
      resp <- makeApp(Nil, ref).run(req)
    yield expect(resp.status == Status.BadRequest)
  }

  test("GET / returns 503 when loadAll fails") {
    for
      ref <- Ref.of[IO, List[EventStoreNotification]](Nil)
      app  = new CheckpointRoutes[IO](
              IO.raiseError(new RuntimeException("DB down")),
              n => ref.update(_ :+ n),
            ).routes.orNotFound
      resp <- app.run(Request[IO](Method.GET, uri"/"))
    yield expect(resp.status == Status.ServiceUnavailable)
  }

  test("POST /checkpoints/{name}/pause returns 503 when sendNotification fails") {
    val app = new CheckpointRoutes[IO](
      IO.pure(Nil),
      _ => IO.raiseError(new RuntimeException("notify failed")),
    ).routes.orNotFound
    for resp <- app.run(Request[IO](Method.POST, uri"/checkpoints/books/pause"))
    yield expect(resp.status == Status.ServiceUnavailable)
  }

  test("POST /checkpoints/{name}/resume returns 503 when sendNotification fails") {
    val app = new CheckpointRoutes[IO](
      IO.pure(Nil),
      _ => IO.raiseError(new RuntimeException("notify failed")),
    ).routes.orNotFound
    for resp <- app.run(Request[IO](Method.POST, uri"/checkpoints/books/resume"))
    yield expect(resp.status == Status.ServiceUnavailable)
  }

  test("POST /checkpoints/{name}/index returns 503 when sendNotification fails") {
    val body = UrlForm("index" -> "1")
    val req = Request[IO](Method.POST, uri"/checkpoints/books/index").withEntity(body)
    val app = new CheckpointRoutes[IO](
      IO.pure(Nil),
      _ => IO.raiseError(new RuntimeException("notify failed")),
    ).routes.orNotFound
    for resp <- app.run(req)
    yield expect(resp.status == Status.ServiceUnavailable)
  }
