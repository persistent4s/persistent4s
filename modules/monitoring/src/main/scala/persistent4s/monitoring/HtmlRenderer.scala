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

import persistent4s.ProjectionCheckpointState

object HtmlRenderer:

  def render(states: List[ProjectionCheckpointState]): String =
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |  <meta charset="UTF-8">
       |  <title>Projection Checkpoints</title>
       |  <style>
       |    body { font-family: sans-serif; padding: 2rem; }
       |    table { border-collapse: collapse; width: 100%; }
       |    th, td { border: 1px solid #ccc; padding: 0.5rem 1rem; text-align: left; }
       |    .running { color: green; font-weight: bold; }
       |    .paused  { color: red;   font-weight: bold; }
       |    pre { margin: 0; font-size: 0.8em; white-space: pre-wrap; }
       |  </style>
       |</head>
       |<body>
       |  <h1>Projection Checkpoints</h1>
       |  <table>
       |    <thead>
       |      <tr><th>Name</th><th>Position</th><th>Status</th><th>Error</th><th>Actions</th></tr>
       |    </thead>
       |    <tbody>
       |      ${states.map(renderRow).mkString("\n      ")}
       |    </tbody>
       |  </table>
       |</body>
       |</html>""".stripMargin

  def renderError(msg: String): String =
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head><meta charset="UTF-8"><title>Projection Checkpoints — Error</title></head>
       |<body>
       |  <h1>Projection Checkpoints</h1>
       |  <p style="color:red">Error loading checkpoints: ${escape(msg)}</p>
       |</body>
       |</html>""".stripMargin

  private def renderRow(s: ProjectionCheckpointState): String =
    val statusClass = if s.running then "running" else "paused"
    val statusText = if s.running then "Running" else "Paused"
    val errorCell = s.error.fold("&nbsp;")(e => s"<pre>${escape(e)}</pre>")
    val enc = java.net.URLEncoder.encode(s.projectionName, java.nio.charset.StandardCharsets.UTF_8)
    s"""<tr>
       |        <td>${escape(s.projectionName)}</td>
       |        <td>${s.globalPosition}</td>
       |        <td class="$statusClass">$statusText</td>
       |        <td>$errorCell</td>
       |        <td>
       |          <form method="post" action="/checkpoints/$enc/pause" style="display:inline">
       |            <button type="submit">Pause</button>
       |          </form>
       |          <form method="post" action="/checkpoints/$enc/resume" style="display:inline">
       |            <button type="submit">Resume</button>
       |          </form>
       |          <form method="post" action="/checkpoints/$enc/index" style="display:inline">
       |            <input type="number" name="index" value="${s.globalPosition}" style="width:6rem">
       |            <button type="submit">Set index</button>
       |          </form>
       |        </td>
       |      </tr>""".stripMargin

  private def escape(s: String): String =
    s.replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
