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

package persistent4s

/** Runs a singleton background loop under mutual exclusion so the same code can run on every instance of a horizontally
  * scaled service. Each logical loop is identified by a stable `name`; across all instances at most one holds
  * leadership for a given name at a time and runs its `task`, while the others stand by and take over automatically if
  * the leader releases, stops, or crashes.
  */
trait LeaderElection[F[_]]:

  /** Acquire leadership for `name`, run `task` while holding it, and release it when `task` finishes, fails, or is
    * cancelled. While another instance holds leadership, stand by and retry until it is acquired.
    */
  def runAsLeader(name: String)(task: F[Unit]): F[Unit]
