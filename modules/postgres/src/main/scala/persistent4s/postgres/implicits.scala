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

package persistent4s.postgres

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import natchez.Trace.Implicits.noop

import persistent4s.EventCodec

object implicits:

  private val defaultConfig = PostgresEventStore.Config(
    host = "localhost", port = 5450, database = "persistent4s", user = "persistent4s", password = "persistent4s",
  )

  implicit def store[A](implicit codec: EventCodec[A]): PostgresEventStore[IO, A] =
    PostgresEventStore.make[IO, A](defaultConfig, codec).allocated.unsafeRunSync()._1
