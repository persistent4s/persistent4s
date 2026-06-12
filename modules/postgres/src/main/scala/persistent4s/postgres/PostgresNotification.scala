/*
 * Copyright 2026 Bastien Jolidon
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

import persistent4s.EventStoreNotification

object PostgresNotification {

  def encode(n: EventStoreNotification): String =
    n match {
      case EventStoreNotification.EventsAppended =>
        "events_appended"

      case EventStoreNotification.PauseProjection(name) =>
        s"pause_projection:$name"

      case EventStoreNotification.ResumeProjection(name) =>
        s"resume_projection:$name"

      case EventStoreNotification.UpdateCheckpointIndex(name, index) =>
        s"update_checkpoint_index:$name:$index"

      case _ =>
        "unknown_notification"
    }

  def decode(s: String): EventStoreNotification =
    s match {
      case "events_appended" =>
        EventStoreNotification.EventsAppended

      case str if str.startsWith("pause_projection:") =>
        EventStoreNotification.PauseProjection(str.stripPrefix("pause_projection:"))

      case str if str.startsWith("resume_projection:") =>
        EventStoreNotification.ResumeProjection(str.stripPrefix("resume_projection:"))

      case str if str.startsWith("update_checkpoint_index:") =>
        val rest = str.stripPrefix("update_checkpoint_index:")
        rest.lastIndexOf(':') match {
          case -1 =>
            EventStoreNotification.UnknownNotification

          case idx =>
            val name = rest.substring(0, idx)
            val indexStr = rest.substring(idx + 1)

            indexStr.toLongOption match {
              case Some(index) =>
                EventStoreNotification.UpdateCheckpointIndex(name, index)
              case None =>
                EventStoreNotification.UnknownNotification
            }
        }

      case _ =>
        EventStoreNotification.UnknownNotification
    }

}
