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

/** A point in the order in which an [[EventStore]] delivers events to projections: by `commitGroup`, then by
  * `globalPosition`.
  *
  * A store whose positions are allocated before their writes commit cannot deliver in position order without risking
  * that a lower position becomes visible after a higher one was already handed out. Such a store groups the events of
  * each write into a commit group and only delivers a group once no earlier group can still appear. Stores that do not
  * need this put every event in commit group 0, which reduces delivery order to plain position order.
  *
  * @param commitGroup
  *   the delivery group of the event this cursor points at; 0 for stores without commit groups
  * @param globalPosition
  *   the position of the event this cursor points at
  */
final case class ReadCursor(commitGroup: Long, globalPosition: Long)

object ReadCursor:

  /** Before every event. The `-1` position is the "no checkpoint yet" sentinel checkpoint stores rely on. */
  val Start: ReadCursor = ReadCursor(0L, -1L)
