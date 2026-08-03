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

/** Stable serialization for a key used in a [[Scope]]. Scope keys become part of durable stream identities, so their
  * encoding must not depend on an unstable `toString` implementation.
  */
trait ScopeKeyEncoder[-K]:

  def encode(key: K): String

/** Stable decoding for scope keys. It is used when infrastructure metadata needs to recover the typed key, for
  * example when a projection is keyed directly by an event's declared scope.
  */
trait ScopeKeyDecoder[+K]:

  def decode(value: String): Either[Throwable, K]

/** A bidirectional durable scope-key format. */
trait ScopeKeyCodec[K] extends ScopeKeyEncoder[K] with ScopeKeyDecoder[K]

object ScopeKeyEncoder:

  def apply[K](using encoder: ScopeKeyEncoder[K]): ScopeKeyEncoder[K] = encoder

  def instance[K](f: K => String): ScopeKeyEncoder[K] =
    new ScopeKeyEncoder[K]:
      def encode(key: K): String = f(key)

  private def codec[K](encodeKey: K => String)(decodeKey: String => Either[Throwable, K]): ScopeKeyCodec[K] =
    new ScopeKeyCodec[K]:
      def encode(key: K): String = encodeKey(key)
      def decode(value: String): Either[Throwable, K] = decodeKey(value)

  given ScopeKeyCodec[String] = codec[String](value => value)(value => Right(value))

  given ScopeKeyCodec[UUID] = codec[UUID](_.toString)(value =>
    try Right(UUID.fromString(value))
    catch case error: IllegalArgumentException => Left(error),
  )

  given ScopeKeyCodec[Int] =
    codec[Int](_.toString)(value => value.toIntOption.toRight(new IllegalArgumentException(value)))

  given ScopeKeyCodec[Long] =
    codec[Long](_.toString)(value => value.toLongOption.toRight(new IllegalArgumentException(value)))

/** A resolved, storage-ready scope identity. The definition name and encoded key are kept separately so storage
  * implementations do not need to parse [[value]].
  */
final case class ScopeId private (name: String, key: String):

  /** Canonical textual representation used by stores that persist scopes as a single value. */
  def value: String = s"$name:$key"

  /** Compatibility bridge for stores that still represent consistency boundaries as [[Tag]] values. */
  def toTag: Tag = Tag(name, key)

object ScopeId:

  /** Parse the canonical `name:key` representation. The key may itself contain `:` characters. */
  def fromString(value: String): Option[ScopeId] =
    value.split(":", 2) match
      case Array(name, key) if validName(name) && key.nonEmpty => Some(new ScopeId(name, key))
      case _                                                   => None

  private[persistent4s] def make(name: String, key: String): ScopeId =
    require(validName(name), "Scope name must be non-empty and cannot contain ':'")
    require(key.nonEmpty, "Encoded scope key must be non-empty")
    new ScopeId(name, key)

  private def validName(name: String): Boolean =
    name.nonEmpty && !name.contains(':')

/** A typed, stable scope definition. A scope identifies the event history and consistency boundary for one key.
  * Define it once and resolve it with a correctly typed key:
  *
  * {{{
  * val books = Scope[UUID]("library.book")
  * val bookScope: ScopeId = books(bookId)
  * }}}
  *
  * The name is a durable identifier and must not be changed after events have been written without a migration.
  */
final class Scope[K] private (
  val name: String,
  encoder: ScopeKeyEncoder[K],
  decoder: Option[ScopeKeyDecoder[K]],
):

  /** Resolve this definition for a concrete key. */
  def apply(key: K): ScopeId =
    ScopeId.make(name, encoder.encode(key))

  /** Decode a resolved identity back to this scope's typed key. Custom encode-only scopes return a descriptive error. */
  def decode(id: ScopeId): Either[Throwable, K] =
    if id.name != name then Left(new IllegalArgumentException(s"Scope ${id.name} cannot be decoded as $name"))
    else
      decoder
        .toRight(new IllegalStateException(s"Scope $name was defined without a key decoder"))
        .flatMap(_.decode(id.key))

  override def toString: String = s"Scope($name)"

object Scope:

  /** Define a scope using the stable key encoder available for `K`. */
  def apply[K: ScopeKeyEncoder](name: String): Scope[K] =
    require(name.nonEmpty, "Scope name must be non-empty")
    require(!name.contains(':'), "Scope name cannot contain ':'")
    val encoder = ScopeKeyEncoder[K]
    val decoder = encoder match
      case value: ScopeKeyDecoder[?] => Some(value.asInstanceOf[ScopeKeyDecoder[K]])
      case _                         => None
    new Scope(name, encoder, decoder)

  /** Define a scope with an explicit stable key encoder. */
  def encoded[K](name: String)(encode: K => String): Scope[K] =
    apply(name)(using ScopeKeyEncoder.instance(encode))

  /** Define a custom bidirectional durable scope-key format. */
  def codec[K](name: String)(encodeKey: K => String)(decodeKey: String => Either[Throwable, K]): Scope[K] =
    val keyCodec = new ScopeKeyCodec[K]:
      def encode(key: K): String = encodeKey(key)
      def decode(value: String): Either[Throwable, K] = decodeKey(value)
    apply(name)(using keyCodec)
