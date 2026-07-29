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

import java.time.Instant
import java.util.UUID

/** Lifecycle state of a saga instance. Only `Pending` instances accept replies or fire deadlines */
enum SagaStatus:

  case Pending, Completed, Compensated, Failed

object SagaStatus:

  def fromString(s: String): Option[SagaStatus] = SagaStatus.values.find(_.toString == s)

/** A persisted saga instance with its state. */
final case class SagaRecord(
  id: UUID,
  sagaName: String,
  key: String,
  status: SagaStatus,
  step: Int,
  data: String,
  deadline: Option[Instant],
)

/** Durable storage for saga instances. */
trait SagaRepository[F[_]]:

  /** Insert `record` and enqueue `message` in one transaction, so an instance is never pending without its request
    * having been enqueued, and a request is never sent without a row to route the reply to.
    *
    * @return
    *   false if an instance with that id already exists
    */
  def start(record: SagaRecord, messages: List[OutgoingMessage]): F[Boolean]

  def find(id: UUID): F[Option[SagaRecord]]

  /** Move an instance forward, conditional on it still being [[SagaStatus.Pending]] at `expectedStep`. Both halves of
    * that condition matter: a terminal outcome does not have to bump the step, so a redelivered reply can arrive with a
    * matching `expectedStep` on an already-`Completed` instance and must not be applied twice.
    *
    * @return
    *   false if the instance was no longer pending at `expectedStep`, i.e. this reply or deadline is a duplicate
    */
  def advance(
    id: UUID,
    expectedStep: Int,
    status: SagaStatus,
    step: Int,
    data: String,
    deadline: Option[Instant],
  ): F[Boolean]

  /** Claim up to `limit` pending instances of `sagaName` whose deadline has passed and hand them to `handle`.
    *
    * @return
    *   the number of instances handed over
    */
  def claimExpired(sagaName: String, limit: Int)(handle: List[SagaRecord] => F[Unit]): F[Int]
