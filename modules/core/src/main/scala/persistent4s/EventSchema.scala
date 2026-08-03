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

/** Stable storage identity and current schema version for an event type `E`.
  *
  * Unlike [[EventTypeName.of]], the identifier is not derived from a Scala class name and therefore survives class and
  * package renames. Versions start at `1` and are incremented whenever persisted payloads require an upcast before they
  * can be decoded as the current event type.
  *
  * Aliases allow an existing stored identifier to be migrated to a stable identifier without rewriting old rows.
  */
final case class EventSchema[E] private (
  eventType: EventTypeName,
  version: Int,
  aliases: Set[EventTypeName],
  private val scopeResolvers: Map[String, E => List[ScopeId]],
):

  require(eventType.value.nonEmpty, "Event type identifier must be non-empty")
  require(version >= 1, "Event schema version must be at least 1")
  require(!aliases.contains(eventType), "An event schema alias must differ from its primary identifier")

  /** Every stored identifier accepted when decoding this event. */
  def acceptedEventTypes: Set[EventTypeName] = aliases + eventType

  /** Stable names of every scope carried by this event. */
  def scopeNames: Set[String] = scopeResolvers.keySet

  /** Accept an additional historical identifier when decoding. */
  def withAlias(alias: String): EventSchema[E] =
    require(alias.nonEmpty, "Event type alias must be non-empty")
    val eventTypeAlias = EventTypeName.fromString(alias)
    if eventTypeAlias == eventType then this else copy(aliases = aliases + eventTypeAlias)

  /** Declare that this event belongs to `scope`, deriving its typed key from the event payload. */
  def scopedBy[K](scope: Scope[K])(key: E => K): EventSchema[E] =
    scopedByMany(scope)(event => key(event) :: Nil)

  /** Declare that this event belongs to several keys in the same durable scope. */
  def scopedByMany[K](scope: Scope[K])(keys: E => IterableOnce[K]): EventSchema[E] =
    require(!scopeResolvers.contains(scope.name), s"Event ${eventType.value} already declares scope ${scope.name}")
    copy(scopeResolvers =
      scopeResolvers.updated(scope.name, event => keys(event).iterator.map(scope(_)).toList.distinct),
    )

  /** Resolve one declared scope by its stable name. */
  def resolveScope(scopeName: String, event: E): Option[ScopeId] =
    resolveScopeIds(scopeName, event).headOption

  /** Resolve one declared scope using its typed definition. */
  def resolveScope[K](scope: Scope[K], event: E): Option[ScopeId] =
    resolveScope(scope.name, event)

  /** Resolve every key declared for one scope name. */
  def resolveScopeIds(scopeName: String, event: E): List[ScopeId] =
    scopeResolvers.get(scopeName).fold(List.empty[ScopeId])(_(event))

  /** Resolve every key declared for one typed scope. */
  def resolveScopeIds[K](scope: Scope[K], event: E): List[ScopeId] =
    resolveScopeIds(scope.name, event)

  /** Resolve every scope carried by this event. */
  def resolveScopes(event: E): Set[ScopeId] =
    scopeResolvers.valuesIterator.flatMap(_(event)).toSet

object EventSchema:

  /** Define a stable event identifier and its current payload version. */
  def apply[E](eventType: String, version: Int = 1): EventSchema[E] =
    new EventSchema(EventTypeName.fromString(eventType), version, Set.empty, Map.empty)

  /** Class-name-based schema used when no explicit [[EventSchema]] is available. This preserves the legacy codec
    * behavior while allowing applications to adopt stable identifiers incrementally.
    */
  def legacy[E](using classTag: ClassTag[E]): EventSchema[E] =
    EventSchema(classTag.runtimeClass.getSimpleName, version = 1)

/** Serialized event payload together with the stable metadata needed to decode it later. */
final case class EncodedEvent(
  eventType: EventTypeName,
  version: Int,
  payload: String,
)

/** The event identity and payload version a concrete store will persist. */
final case class EventStorageSchema(
  eventType: EventTypeName,
  version: Int,
)

/** Raised when a handler/caller and its concrete event store disagree about an event's durable schema. */
final case class EventSchemaMismatch(
  declared: EventStorageSchema,
  storage: EventStorageSchema,
  eventClass: String,
) extends IllegalArgumentException(
      s"Event schema mismatch for $eventClass: handler declared ${declared.eventType.value}@${declared.version}, " +
        s"but the store codec uses ${storage.eventType.value}@${storage.version}",
    )

/** A legacy [[EventCodec]] was asked to decode a version it cannot evolve. */
final case class UnsupportedEventVersion(
  eventType: EventTypeName,
  version: Int,
) extends IllegalArgumentException(
      s"Event ${eventType.value} has unsupported schema version $version",
    )
