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

import cats.Applicative
import cats.syntax.all.*

import scala.reflect.ClassTag

/** A handler for a single event type, registered by an [[EventSourcedProjection]]. It bundles the event type it
  * applies to, how to resolve the affected keys from a payload, and how to fold that payload into the state. Build one
  * with [[EventSourcedProjection.on]], [[EventSourcedProjection.onF]] or [[EventSourcedProjection.onMany]].
  */
final class EventHandler[F[_], A <: Event, K, S] private (
  private[persistent4s] val eventType: EventTypeName,
  private val resolveKey: A => List[K],
  private val run: (Option[S], A) => F[Option[S]],
):

  private[persistent4s] def resolveKeys(payload: A): List[K] = resolveKey(payload)

  private[persistent4s] def handle(state: Option[S], payload: A): F[Option[S]] = run(state, payload)

object EventHandler:

  private[persistent4s] def make[F[_], A <: Event, E <: A, K, S](
    keys: E => List[K],
    f: (Option[S], E) => F[Option[S]],
  )(using ClassTag[E]): EventHandler[F, A, K, S] =
    new EventHandler[F, A, K, S](
      EventTypeName.of[E],
      payload => keys(payload.asInstanceOf[E]),
      (state, payload) => f(state, payload.asInstanceOf[E]),
    )

/** A [[Projection]] defined as a set of per-event handlers rather than three parallel matches. Each event type is
  * declared once with [[on]], [[onF]] or [[onMany]]; from that single declaration the trait derives `filter` (so it can
  * never drift from the handlers), `resolveKeys`, and `handle`. Events with no registered handler are ignored.
  *
  * Implementors provide [[name]], [[repository]] (inherited from [[Projection]]) and [[handlers]].
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  * @tparam K
  *   the state key type
  * @tparam S
  *   the state value type
  */
trait EventSourcedProjection[F[_]: Applicative, A <: Event, K, S] extends Projection[F, A, K, S]:

  /** The handlers for this projection, one per event type. Declare each with [[on]], [[onF]] or [[onMany]]. */
  def handlers: List[EventHandler[F, A, K, S]]

  /** Register a pure handler for events of type `E` that affect a single key. */
  final protected def on[E <: A: ClassTag](key: E => K)(f: (Option[S], E) => Option[S]): EventHandler[F, A, K, S] =
    EventHandler.make[F, A, E, K, S](e => List(key(e)), (s, e) => f(s, e).pure)

  /** Register an effectful handler for events of type `E` that affect a single key. */
  final protected def onF[E <: A: ClassTag](key: E => K)(f: (Option[S], E) => F[Option[S]]): EventHandler[F, A, K, S] =
    EventHandler.make[F, A, E, K, S](e => List(key(e)), f)

  /** Register an effectful handler for events of type `E` that fan out to several keys. */
  final protected def onMany[E <: A: ClassTag](
    keys: E => List[K],
  )(f: (Option[S], E) => F[Option[S]]): EventHandler[F, A, K, S] =
    EventHandler.make[F, A, E, K, S](keys, f)

  private lazy val byType: Map[EventTypeName, EventHandler[F, A, K, S]] =
    handlers.map(h => h.eventType -> h).toMap

  final override def filter: Set[EventTypeName] =
    byType.keySet

  final override def resolveKeys(event: EventEnvelope[A]): List[K] =
    byType.get(event.metadata.eventType).fold(List.empty[K])(_.resolveKeys(event.payload))

  final override def handle(state: Option[S], event: EventEnvelope[A]): F[Option[S]] =
    byType.get(event.metadata.eventType).fold(state.pure)(_.handle(state, event.payload))
