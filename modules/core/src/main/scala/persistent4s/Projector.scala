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

package persistent4s

import fs2.Stream

/** A Projector is responsible for running a Projection. It manages the lifecycle of the projection, including loading
  * the last checkpoint, subscribing to event notifications, and ensuring that events are processed in the correct
  * order. The projector should handle any necessary error handling and retries to ensure that the projection processes
  * events reliably.
  */
trait Projector[F[_], A]:

  /** Run the given projection. This should start the projection and keep it running, processing events as they come in.
    *
    * @param projection
    *   the projection to run
    */
  def run(projection: Projection[F, A]): Stream[F, Unit]
