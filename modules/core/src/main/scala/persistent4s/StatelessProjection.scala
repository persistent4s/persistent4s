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

/** A [[Projection]] that reacts to events with side effects (e.g. updating a read model) without keeping any state.
  * Implementors provide [[name]], [[filter]], and a single-argument [[handle]]; key resolution and state persistence
  * are no-ops.
  *
  * Delivery is at-least-once, so `handle` may run more than once for the same event after a restart — keep it
  * idempotent.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  */
trait StatelessProjection[F[_]: Applicative, A <: Event] extends Projection[F, A, Unit, Unit] {

  final val repository: Repository[F, Unit, Unit] = Repository.empty

  override def resolveKeys(event: EventEnvelope[A]): List[Unit] = List(())

  def handle(event: EventEnvelope[A]): F[Unit]

  final def handle(state: Option[Unit], event: EventEnvelope[A]): F[Option[Unit]] =
    handle(event).as(state)

}
