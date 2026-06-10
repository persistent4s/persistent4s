package persistent4s

import java.util.UUID

/** A PendingEvent is an event that is about to be appended to the event store, together with the author-supplied
  * context the store needs to persist it. Unlike [[EventMetadata]], it carries no store-assigned fields
  * (globalPosition, timestamp) — those are assigned at commit time.
  *
  * @param payload
  *   the actual event data
  * @param tags
  *   the tags associated with the event
  * @param eventType
  *   the type of the event
  * @param isExternal
  *   whether the event comes from an external domain
  * @param id
  *   an optional caller-supplied UUID
  * @param headers
  *   arbitrary author-supplied key-value metadata
  */
final case class PendingEvent[A <: Event](
  payload: A,
  tags: Set[Tag],
  eventType: EventTypeName,
  isExternal: Boolean,
  id: Option[UUID] = None,
  headers: Map[String, String] = Map.empty,
)
