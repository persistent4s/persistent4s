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

package persistent4s.examples.saga.orders.domain.order

import java.util.UUID

import scala.util.Try

import cats.effect.{Async, Resource}
import io.circe.Encoder
import skunk.Session
import skunk.codec.all.text

import persistent4s.postgres.{DerivedPostgresRepository, PostgresField, PostgresTable}

/** Where an order stands. `Placed` is not a step towards being recorded — it is a recorded state an order can sit in
  * for as long as the saga takes, and the state a client will usually see if it reads immediately after posting.
  */
enum OrderStatus:

  case Placed, Confirmed, Cancelled

object OrderStatus:

  /** A plain string over the wire — circe's sum derivation would wrap it in an object, and `"status":"Placed"` is what
    * a reader of this API expects.
    */
  given Encoder[OrderStatus] = Encoder.encodeString.contramap(_.toString)

  /** How the column is stored. A derived row summons one `PostgresField` per case-class field and has no default for a
    * domain enum, so this is where the mapping lives — one `text` column, not a nested product.
    */
  given PostgresField[OrderStatus] =
    PostgresField(
      text.eimap(name => Try(OrderStatus.valueOf(name)).toEither.left.map(_ => s"Unknown order status: $name"))(
        _.toString,
      ),
    )

final case class OrderView(
  orderId: UUID,
  customerId: UUID,
  itemId: UUID,
  amount: Int,
  status: OrderStatus,
  reason: Option[String],
) derives Encoder.AsObject

/** `findMany`, `persist`, `find`, `all` and the atomic checkpoint commit all come from the derived mapping. The table
  * name stays explicit so renaming the Scala type cannot rename a live table.
  */
final class OrderRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends DerivedPostgresRepository[F, UUID, OrderView](pool, OrderRepository.table)

object OrderRepository:

  private val table: PostgresTable[UUID, OrderView] =
    PostgresTable.derived[OrderView]("orders").key(_.orderId)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): OrderRepository[F] =
    new OrderRepository(pool)
