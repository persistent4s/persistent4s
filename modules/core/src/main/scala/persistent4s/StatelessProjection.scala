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

/** A convenience base trait for projections that perform side effects (e.g. updating a read model) without needing
  * keyed state management.
  *
  * Unlike the full [[Projection]] trait, `StatelessProjection` does not fetch or persist any state between events.
  * Implementors only need to provide [[name]], [[filter]], and a single-argument [[handle]] that receives the raw
  * event. All events are routed through a single virtual key (`Unit`), so `fetchState`, `resolveKeys`, and `persist`
  * are all no-ops.
  *
  * Because there is no state, the at-least-once delivery guarantee still applies: `handle` may be called more than once
  * for the same event after a failure and restart, so implementations should be idempotent.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  */
trait StatelessProjection[F[_]: Applicative, A <: Event] extends Projection[F, A, scala.Unit] {

  type State = scala.Unit

  override def resolveKeys(event: EventEnvelope[A]): List[Unit] = List(())

  override def fetchState(key: Unit): F[Option[State]] = Applicative[F].pure(None)

  def handle(event: EventEnvelope[A]): F[Unit]

  final def handle(state: Option[Unit], event: EventEnvelope[A]): F[Option[Unit]] =
    handle(event).as(state)

  override def persist(key: Unit, state: Option[Unit]): F[Unit] = Applicative[F].unit

}
