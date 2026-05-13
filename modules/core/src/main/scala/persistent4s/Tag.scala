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

import java.util.UUID

/** A Tag is a simple key-value pair that can be associated with an event in the event store. Tags are used to
  * categorize and filter events when reading from the event store. Each tag consists of a category and an identifier,
  * and the value of the tag is a string in the format "category:id". The Tag class provides a method to convert a Tag
  * to its string representation, as well as a companion object with a method to parse a Tag from a string.
  *
  * @param category
  *   the category of the tag
  * @param id
  *   the identifier of the tag
  */
final case class Tag(category: String, id: String):

  def value: String = s"$category:$id"

object Tag:

  def apply(category: String, id: UUID): Tag =
    Tag(category, id.toString)

  def fromString(s: String): Option[Tag] =
    s.split(":", 2) match
      case Array(category, id) => Some(Tag(category, id))
      case _                   => None

  def setToString(tags: Set[Tag]): String =
    tags.map(_.value).mkString(",")

  def stringToSet(s: String): Set[Tag] =
    s.split(",").flatMap(fromString).toSet
