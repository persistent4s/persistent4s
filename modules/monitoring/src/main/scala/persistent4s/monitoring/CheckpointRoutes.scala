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

import cats.effect.Async
import cats.syntax.all.*

import persistent4s.{EventStoreNotification, ProjectionCheckpointState}

import org.http4s.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Location, `Content-Type`}
import org.http4s.implicits.*

final class CheckpointRoutes[F[_]: Async](
  loadAll: F[List[ProjectionCheckpointState]],
  sendNotification: EventStoreNotification => F[Unit],
) extends Http4sDsl[F]:

  val routes: HttpRoutes[F] = HttpRoutes.of[F] {

    case GET -> Root =>
      loadAll.flatMap { states =>
        Ok(HtmlRenderer.render(states))
          .map(_.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`)))
      }.handleErrorWith { e =>
        ServiceUnavailable(HtmlRenderer.renderError(e.getMessage))
          .map(_.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`)))
      }

    case GET -> Root / "checkpoints" / "data" =>
      loadAll
        .flatMap(states => Ok(HtmlRenderer.renderJson(states)))
        .map(_.withContentType(`Content-Type`(MediaType.application.json, Charset.`UTF-8`)))
        .handleErrorWith(_ => Ok("[]"))

    case POST -> Root / "checkpoints" / name / "pause" =>
      sendNotification(EventStoreNotification.PauseProjection(name))
        .flatMap(_ => SeeOther(Location(uri"/")))
        .handleErrorWith(_ =>
          ServiceUnavailable(HtmlRenderer.renderError("Failed to send notification"))
            .map(_.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))),
        )

    case POST -> Root / "checkpoints" / name / "resume" =>
      sendNotification(EventStoreNotification.ResumeProjection(name))
        .flatMap(_ => SeeOther(Location(uri"/")))
        .handleErrorWith(_ =>
          ServiceUnavailable(HtmlRenderer.renderError("Failed to send notification"))
            .map(_.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))),
        )

    case req @ POST -> Root / "checkpoints" / name / "index" =>
      req.as[UrlForm].flatMap { form =>
        form.getFirst("index").flatMap(_.toLongOption) match
          case None      => BadRequest("index must be a valid Long")
          case Some(idx) =>
            sendNotification(EventStoreNotification.UpdateCheckpointIndex(name, idx))
              .flatMap(_ => SeeOther(Location(uri"/")))
              .handleErrorWith(_ =>
                ServiceUnavailable(HtmlRenderer.renderError("Failed to send notification"))
                  .map(_.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))),
              )
      }
  }
