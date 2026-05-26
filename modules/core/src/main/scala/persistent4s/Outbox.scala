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

import fs2.Stream

/** Transactional outbox for events that still need to be published to an external broker (e.g. Kafka).
  *
  * An [[EventStore]] implementation is expected to enqueue a row into the outbox in the same transaction that appends
  * the event, so an event either becomes visible together with its outbox entry or not at all. A relay process then
  * streams unpublished entries via [[stream]], publishes them, and acknowledges with [[markPublished]].
  *
  * The trait deliberately knows nothing about Kafka. Any broker-specific relay can consume from it.
  *
  * @tparam F
  *   the effect type
  * @tparam A
  *   the event type
  */
trait Outbox[F[_], A <: Event]:

  /** Stream unpublished outbox entries in the order they became visible to the outbox.
    *
    * Implementations typically poll the underlying storage, optionally woken up by [[notifications]]. The stream should
    * not terminate; it stays open until cancelled by the consumer.
    *
    * ==Ordering==
    *
    * Entries are emitted in ascending `globalPosition` '''of currently-visible rows''', which corresponds to
    * transaction commit order (not necessarily the order in which `globalPosition`s were originally allocated). When
    * two appending transactions run concurrently and the higher-positioned one commits first, the higher-positioned
    * entry is emitted before the lower one (which only becomes visible after its transaction commits).
    *
    * This matches the per-tag ordering guarantee exposed by `EventSubscriber`: events sharing a tag scope are
    * serialized at append time by the event store, so within any tag scope this stream is in `globalPosition` order.
    * Across disjoint tag scopes the order is the relay's-eye view of commit order.
    *
    * @param batchSize
    *   maximum number of entries to fetch per round-trip
    */
  def stream(batchSize: Int): Stream[F, EventEnvelope[A]]

  /** Mark a single entry as published. After this call returns successfully, the entry must no longer be emitted by
    * [[stream]]. How implementations achieve that (by deleting the row, stamping a `published_at` column, moving it to
    * an archive table, etc.) is a backend choice. Idempotent: marking an already-published or unknown entry is a no-op.
    */
  def markPublished(globalPosition: Long): F[Unit]

  /** Mark a batch of entries as published in one round-trip. Same semantics as the single-entry version. */
  def markPublished(globalPositions: List[Long]): F[Unit]

  /** Wake-up signal emitted whenever new entries may be available. Implementations that cannot provide a signal should
    * return `Stream.empty`; consumers must then rely on polling.
    */
  def notifications: Stream[F, Unit]
