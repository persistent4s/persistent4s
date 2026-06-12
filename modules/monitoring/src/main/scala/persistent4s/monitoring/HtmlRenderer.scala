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

import persistent4s.ProjectionCheckpointState

object HtmlRenderer:

  def renderRows(states: List[ProjectionCheckpointState]): String =
    states.map(renderRow).mkString("\n      ")

  def renderJson(states: List[ProjectionCheckpointState]): String =
    val items = states.map { s =>
      val errorJson = s.error.fold("null")(e => s"\"${escapeJson(e)}\"")
      s"""{"name":"${escapeJson(
          s.projectionName,
        )}","position":${s.globalPosition},"running":${s.running},"error":$errorJson}"""
    }
    s"[${items.mkString(",")}]"

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
       |    details summary { cursor: pointer; font-size: 0.9em; }
       |  </style>
       |</head>
       |<body>
       |  <h1>Projection Checkpoints</h1>
       |  <table>
       |    <thead>
       |      <tr><th>Name</th><th>Position</th><th>Status</th><th>Error</th><th>Actions</th></tr>
       |    </thead>
       |    <tbody id="checkpoint-tbody">
       |      ${renderRows(states)}
       |    </tbody>
       |  </table>
       |  <p><small>Last updated: <span id="last-updated">—</span></small></p>
       |  <script>
       |    setInterval(function() {
       |      fetch('/checkpoints/data')
       |        .then(function(r) { return r.json(); })
       |        .then(function(data) {
       |          data.forEach(function(s) {
       |            var id = s.name.replace(/[^a-zA-Z0-9]/g, '-');
       |            var pos    = document.getElementById('pos-'    + id);
       |            var status = document.getElementById('status-' + id);
       |            var err    = document.getElementById('error-'  + id);
       |            if (pos) pos.textContent = s.position;
       |            if (status) {
       |              status.textContent = s.running ? 'Running' : 'Paused';
       |              status.className   = s.running ? 'running' : 'paused';
       |            }
       |            if (err) {
       |              if (s.error) {
       |                var lines   = s.error.split('\\n');
       |                var summary = document.createElement('summary');
       |                summary.textContent = lines[0];
       |                var pre = document.createElement('pre');
       |                pre.textContent = lines.slice(1).join('\\n');
       |                var details = document.createElement('details');
       |                var open = err.querySelector('details') && err.querySelector('details').open;
       |                if (open) details.open = true;
       |                details.appendChild(summary);
       |                details.appendChild(pre);
       |                err.innerHTML = '';
       |                err.appendChild(details);
       |              } else {
       |                err.innerHTML = '&nbsp;';
       |              }
       |            }
       |          });
       |          document.getElementById('last-updated').textContent = new Date().toLocaleTimeString();
       |        })
       |        .catch(function(e) { console.error('Polling error:', e); });
       |    }, 2000);
       |  </script>
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
    val errorCell = s.error.fold("&nbsp;") { e =>
      val lines = e.split("\n", -1)
      val summary = escape(lines.head)
      val rest = escape(lines.tail.mkString("\n"))
      s"""<details><summary>$summary</summary><pre>$rest</pre></details>"""
    }
    val enc = java.net.URLEncoder.encode(s.projectionName, java.nio.charset.StandardCharsets.UTF_8)
    val id = s.projectionName.replaceAll("[^a-zA-Z0-9]", "-")
    s"""<tr>
       |        <td>${escape(s.projectionName)}</td>
       |        <td id="pos-$id">${s.globalPosition}</td>
       |        <td id="status-$id" class="$statusClass">$statusText</td>
       |        <td id="error-$id">$errorCell</td>
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

  private def escapeJson(s: String): String =
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
