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

/** The state changes and checkpoint advance produced by one projection batch.
  *
  * An [[AtomicRepository]] must persist every field in this value atomically. `expectedPosition` is the checkpoint
  * position from which the batch was computed and must be used as an optimistic concurrency check.
  */
final case class ProjectionCommit[K, S](
  upserts: Map[K, S],
  deletes: List[K],
  expectedPosition: Long,
  checkpoint: ProjectionCheckpointState,
)

/** Raised when an atomic projection commit was computed from a checkpoint that is no longer current. */
final case class ProjectionCheckpointConflict(
  projectionName: String,
  expectedPosition: Long,
) extends RuntimeException(
      s"Projection $projectionName is no longer at checkpoint $expectedPosition",
    )

/** A projection repository capable of committing read-model changes and their checkpoint in one transaction.
  *
  * The regular [[Repository.persist]] operation remains available so an atomic repository can still be used by the
  * existing at-least-once path. Projectors automatically use [[persistAtomically]] when this capability is present.
  */
trait AtomicRepository[F[_], K, S] extends Repository[F, K, S]:

  /** Atomically apply the read-model changes and advance the checkpoint.
    *
    * Implementations must fail with [[ProjectionCheckpointConflict]] when the stored checkpoint does not equal
    * `commit.expectedPosition`. On every failure, neither state changes nor the checkpoint may be persisted.
    */
  def persistAtomically(commit: ProjectionCommit[K, S]): F[Unit]

  final override private[persistent4s] def atomicPersist(commit: ProjectionCommit[K, S]): Option[F[Unit]] =
    Some(persistAtomically(commit))
