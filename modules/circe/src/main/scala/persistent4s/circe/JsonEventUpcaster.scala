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

package persistent4s.circe

import scala.util.control.NonFatal

import io.circe.Json

import persistent4s.EventTypeName

/** A stored event is newer than the event schema understood by this application. */
final case class FutureEventVersion(
  eventType: EventTypeName,
  storedVersion: Int,
  currentVersion: Int,
) extends IllegalArgumentException(
      s"Event ${eventType.value} has schema version $storedVersion, but this application only supports up to " +
        s"version $currentVersion",
    )

/** No upcast step was registered for one version in an event's evolution chain. */
final case class MissingEventUpcast(
  eventType: EventTypeName,
  fromVersion: Int,
  currentVersion: Int,
) extends IllegalArgumentException(
      s"Event ${eventType.value} requires an upcast from version $fromVersion to ${fromVersion + 1} " +
        s"before it can be decoded as version $currentVersion",
    )

/** Invalid schema version read from storage. */
final case class InvalidEventVersion(
  eventType: EventTypeName,
  storedVersion: Int,
) extends IllegalArgumentException(
      s"Event ${eventType.value} has invalid schema version $storedVersion; versions start at 1",
    )

/** Ordered JSON transformations for evolving stored payloads of event `E`. Each step transforms exactly one schema
  * version into the next. Upcasters operate on JSON before the current Circe `Decoder[E]` runs, so old event classes do
  * not need to remain in the domain model.
  */
final class JsonEventUpcaster[E] private (
  private val steps: Map[Int, Json => Either[Throwable, Json]],
):

  private[circe] def upcast(
    eventType: EventTypeName,
    fromVersion: Int,
    currentVersion: Int,
    payload: Json,
  ): Either[Throwable, Json] =
    if fromVersion < 1 then Left(InvalidEventVersion(eventType, fromVersion))
    else if fromVersion > currentVersion then Left(FutureEventVersion(eventType, fromVersion, currentVersion))
    else
      def loop(version: Int, currentPayload: Json): Either[Throwable, Json] =
        if version == currentVersion then Right(currentPayload)
        else
          steps.get(version) match
            case None       => Left(MissingEventUpcast(eventType, version, currentVersion))
            case Some(step) =>
              try step(currentPayload).flatMap(next => loop(version + 1, next))
              catch case NonFatal(error) => Left(error)

      loop(fromVersion, payload)

object JsonEventUpcaster:

  /** Start an immutable upcaster definition. */
  def builder[E]: Builder[E] = new Builder(Map.empty)

  final class Builder[E] private[JsonEventUpcaster] (
    private val steps: Map[Int, Json => Either[Throwable, Json]],
  ):

    /** Transform payload version `fromVersion` into `fromVersion + 1`. */
    def step(fromVersion: Int)(transform: Json => Either[Throwable, Json]): Builder[E] =
      require(fromVersion >= 1, "An event upcast step must start at version 1 or later")
      require(!steps.contains(fromVersion), s"Duplicate event upcast step from version $fromVersion")
      new Builder(steps.updated(fromVersion, transform))

    def build: JsonEventUpcaster[E] =
      new JsonEventUpcaster(steps)
