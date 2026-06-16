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

import scala.reflect.ClassTag

/** An Event represents a domain event that has occurred in the system. It is a marker trait that all events must
  * extend. Events are immutable and should contain all the information necessary to understand what happened in the
  * system. They are stored in the event store and can be used to reconstruct the state of the system at any point in
  * time.
  */
trait Event

/** A type-safe wrapper around the event type name string. Using this opaque type instead of raw strings prevents typos
  * when declaring event filters and command handler event types.
  *
  * Instead of writing `"StudentCreated"` (error-prone), users write `EventTypeName.of[StudentCreated]` which is checked
  * by the compiler.
  */
opaque type EventTypeName = String

object EventTypeName:

  /** Create an EventTypeName from a class type. The simple class name is resolved via ClassTag.
    *
    * Example: `EventTypeName.of[StudentCreated]` produces `"StudentCreated"`
    */
  def of[E <: Event](using ct: ClassTag[E]): EventTypeName = ct.runtimeClass.getSimpleName

  /** Create an EventTypeName from a runtime event instance. Used internally when appending events. */
  def fromInstance(a: Event): EventTypeName = a.getClass.getSimpleName

  /** Create an EventTypeName from a raw string. Used for deserialization and storage layer interop. */
  def fromString(s: String): EventTypeName = s

  extension (etn: EventTypeName)

    /** Get the underlying string value. */
    def value: String = etn
