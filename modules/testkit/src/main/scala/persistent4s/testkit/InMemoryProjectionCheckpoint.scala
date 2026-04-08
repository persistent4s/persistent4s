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

package persistent4s.testkit

import persistent4s.ProjectionCheckpoint
import cats.effect.*
import cats.syntax.all.*

final case class InMemoryProjectionCheckpoint[F[_]: Async] private (
  store: Ref[F, Map[String, Long]],
) extends ProjectionCheckpoint[F]:

  override def load(projectionName: String): F[Option[Long]] =
    store.get.map(_.get(projectionName))

  override def save(projectionName: String, globalPosition: Long): F[Unit] =
    store.update(_.updated(projectionName, globalPosition))

object InMemoryProjectionCheckpoint:

  def make[F[_]: Async]: F[InMemoryProjectionCheckpoint[F]] =
    Ref.of[F, Map[String, Long]](Map.empty).map(InMemoryProjectionCheckpoint(_))
