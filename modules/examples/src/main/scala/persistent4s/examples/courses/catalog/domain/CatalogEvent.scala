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

package persistent4s.examples.courses.catalog.domain

import io.circe.{Decoder, Encoder}

import java.util.UUID

import persistent4s.Event

sealed trait CatalogEvent extends Event

final case class CourseOpened(
  courseId: UUID,
  code: String,
  title: String,
  capacity: Int,
  instructor: String,
) extends CatalogEvent derives Encoder, Decoder

final case class CapacityChanged(
  courseId: UUID,
  newCapacity: Int,
) extends CatalogEvent derives Encoder, Decoder

final case class CourseClosed(
  courseId: UUID,
) extends CatalogEvent derives Encoder, Decoder
