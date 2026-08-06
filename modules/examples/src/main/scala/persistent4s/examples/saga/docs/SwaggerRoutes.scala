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

package persistent4s.examples.saga.docs

import cats.effect.IO
import org.http4s.{HttpRoutes, MediaType, StaticFile}
import org.http4s.dsl.io.*
import org.http4s.headers.`Content-Type`

/** Swagger UI for the hand-written routes, so the walkthrough can be clicked instead of curled.
  *
  * The other examples get this from `smithy4s-http4s-swagger`, which can only document a generated service. These
  * routes take the same swagger-ui-dist webjar — already on the classpath as a transitive dependency of that module —
  * and point it at a specification written by hand.
  */
object SwaggerRoutes:

  /** Must match the `org.webjars.npm:swagger-ui-dist` version resolved on the classpath: webjar resources are published
    * under their own version number, so a bump here without a bump there serves 404s.
    */
  private val WebjarRoot = "META-INF/resources/webjars/swagger-ui-dist/5.20.3"

  private val SpecUrl = "/openapi.yaml"

  /** @param specResource
    *   classpath path of the OpenAPI document to serve, e.g. `saga/orders-openapi.yaml`
    */
  def routes(specResource: String, title: String): HttpRoutes[IO] =
    HttpRoutes.of[IO] {
      case GET -> Root / "docs" =>
        Ok(indexHtml(title)).map(_.withContentType(`Content-Type`(MediaType.text.html)))

      case request @ GET -> Root / "openapi.yaml" =>
        StaticFile
          .fromResource[IO](specResource, Some(request))
          .map(_.withContentType(`Content-Type`(MediaType.unsafeParse("application/yaml"))))
          .getOrElseF(NotFound())

      // Single path segment, and a whitelist on it: `fromResource` would happily follow `..` out of the webjar, and a
      // demo is no reason to write a traversal.
      case request @ GET -> Root / "docs" / "assets" / file if isSafeAsset(file) =>
        StaticFile.fromResource[IO](s"$WebjarRoot/$file", Some(request)).getOrElseF(NotFound())
    }

  private def isSafeAsset(file: String): Boolean = file.matches("[A-Za-z0-9._-]+")

  private def indexHtml(title: String): String =
    s"""<!DOCTYPE html>
       |<html lang="en">
       |  <head>
       |    <meta charset="utf-8" />
       |    <title>$title</title>
       |    <link rel="stylesheet" href="/docs/assets/swagger-ui.css" />
       |  </head>
       |  <body>
       |    <div id="swagger-ui"></div>
       |    <script src="/docs/assets/swagger-ui-bundle.js"></script>
       |    <script>
       |      window.ui = SwaggerUIBundle({
       |        url: "$SpecUrl",
       |        dom_id: "#swagger-ui",
       |        tryItOutEnabled: true,
       |        displayRequestDuration: true
       |      });
       |    </script>
       |  </body>
       |</html>
       |""".stripMargin
