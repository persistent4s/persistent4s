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

package persistent4s.monitoring

import cats.effect.IO
import persistent4s.ProjectionCheckpointState
import weaver.SimpleIOSuite

object HtmlRendererSuite extends SimpleIOSuite:

  private val running = ProjectionCheckpointState("books", 42L, true, None)

  private val paused = ProjectionCheckpointState("members", 7L, false, Some("RuntimeException: boom"))

  test("render includes projection names") {
    val html = HtmlRenderer.render(List(running, paused))
    IO.pure(expect.all(html.contains("books"), html.contains("members")))
  }

  test("render shows Running for running projection") {
    IO.pure(expect(HtmlRenderer.render(List(running)).contains("Running")))
  }

  test("render shows Paused for non-running projection") {
    IO.pure(expect(HtmlRenderer.render(List(paused)).contains("Paused")))
  }

  test("render includes global position") {
    IO.pure(expect(HtmlRenderer.render(List(running)).contains("42")))
  }

  test("render includes error message") {
    IO.pure(expect(HtmlRenderer.render(List(paused)).contains("RuntimeException: boom")))
  }

  test("render includes pause form action URL") {
    IO.pure(expect(HtmlRenderer.render(List(running)).contains("/checkpoints/books/pause")))
  }

  test("render includes resume form action URL") {
    IO.pure(expect(HtmlRenderer.render(List(running)).contains("/checkpoints/books/resume")))
  }

  test("render includes index form action URL") {
    IO.pure(expect(HtmlRenderer.render(List(running)).contains("/checkpoints/books/index")))
  }

  test("render set-index input has name=index and pre-filled value") {
    val html = HtmlRenderer.render(List(running))
    IO.pure(
      expect.all(
        html.contains("""name="index""""),
        html.contains("""value="42""""),
      ),
    )
  }

  test("render with empty list contains no status cells") {
    val html = HtmlRenderer.render(Nil)
    IO.pure(expect.all(!html.contains("class=\"running\""), !html.contains("class=\"paused\"")))
  }

  test("renderError includes the error message") {
    IO.pure(expect(HtmlRenderer.renderError("DB is down").contains("DB is down")))
  }

  test("renderError escapes HTML special characters in error message") {
    val html = HtmlRenderer.renderError("<b>bad</b>")
    IO.pure(expect(!html.contains("<b>")))
  }

  test("render escapes HTML special characters in projection name") {
    val state = ProjectionCheckpointState("<script>", 0L, true, None)
    IO.pure(expect(HtmlRenderer.render(List(state)).contains("&lt;script&gt;")))
  }
