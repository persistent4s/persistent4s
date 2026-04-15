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

trait StatelessProjection[F[_]: Applicative, A <: Event] extends Projection[F, A, scala.Unit] {

  type State = scala.Unit

  override def resolveKeys(event: EventEnvelope[A]): List[Unit] = List(())

  override def fetchState(key: Unit): F[Option[State]] = Applicative[F].pure(None)

  def handle(event: EventEnvelope[A]): F[Unit]

  final def handle(state: Option[Unit], event: EventEnvelope[A]): F[Option[Unit]] =
    handle(event).as(state)

  override def persist(key: Unit, state: Option[Unit]): F[Unit] = Applicative[F].unit

}
