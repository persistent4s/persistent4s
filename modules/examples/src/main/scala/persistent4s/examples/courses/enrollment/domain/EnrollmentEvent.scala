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

package persistent4s.examples.courses.enrollment.domain

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Decoder, Encoder}

import persistent4s.Event

sealed trait EnrollmentEvent extends Event

final case class StudentRegistered(
  studentId: UUID,
  name: String,
  email: String,
) extends EnrollmentEvent derives Encoder, Decoder

final case class StudentEnrolled(
  studentId: UUID,
  courseId: UUID,
  enrolledAt: OffsetDateTime,
) extends EnrollmentEvent derives Encoder, Decoder

final case class StudentDropped(
  studentId: UUID,
  courseId: UUID,
  droppedAt: OffsetDateTime,
) extends EnrollmentEvent derives Encoder, Decoder
