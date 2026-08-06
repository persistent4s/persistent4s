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

import cats.effect.*
import cats.syntax.all.*
import io.circe.Encoder
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.Repository

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

final case class OrderView(
  orderId: UUID,
  customerId: UUID,
  itemId: UUID,
  amount: Int,
  status: OrderStatus,
  reason: Option[String],
) derives Encoder.AsObject

final class OrderRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, UUID, OrderView]:

  import OrderRepository.*

  override def findMany(keys: List[UUID]): F[Map[UUID, Option[OrderView]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      pool.use(_.execute(findManyQuery(keys.size))(keys)).map { views =>
        val found = views.map(view => view.orderId -> view).toMap
        keys.map(key => key -> found.get(key)).toMap
      }

  override def persist(upserts: Map[UUID, OrderView], deletes: List[UUID]): F[Unit] =
    if upserts.isEmpty && deletes.isEmpty then Async[F].unit
    else
      pool.use { session =>
        val upsertAll =
          upserts.toList
            .grouped(MaxUpsertChunkSize)
            .toList
            .traverse_(chunk => session.execute(upsertManyCommand(chunk.size))(chunk.map(_._2)).void)

        val deleteAll =
          if deletes.isEmpty then Async[F].unit
          else session.execute(deleteManyCommand(deletes.size))(deletes).void

        session.transaction.use(_ => upsertAll *> deleteAll)
      }

  def find(key: UUID): F[Option[OrderView]] =
    pool.use(_.option(findQuery)(key))

  def getOrders: F[List[OrderView]] =
    pool.use(_.execute(getOrdersQuery))

object OrderRepository:

  private val MaxUpsertChunkSize = 500

  private val statusCodec: Codec[OrderStatus] =
    text.eimap(name => Try(OrderStatus.valueOf(name)).toEither.left.map(_ => s"Unknown order status: $name"))(
      _.toString,
    )

  private val orderViewCodec: Codec[OrderView] =
    (uuid *: uuid *: uuid *: int4 *: statusCodec *: text.opt).to[OrderView]

  private val columns = sql"order_id, customer_id, item_id, amount, status, reason"

  private def findManyQuery(n: Int): Query[List[UUID], OrderView] =
    sql"""
      SELECT $columns
      FROM orders
      WHERE order_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(orderViewCodec)

  private val findQuery: Query[UUID, OrderView] =
    sql"""
      SELECT $columns
      FROM orders
      WHERE order_id = $uuid
    """.query(orderViewCodec)

  private def upsertManyCommand(n: Int): Command[List[OrderView]] =
    sql"""
      INSERT INTO orders ($columns)
      VALUES ${orderViewCodec.values.list(n)}
      ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        item_id     = EXCLUDED.item_id,
        amount      = EXCLUDED.amount,
        status      = EXCLUDED.status,
        reason      = EXCLUDED.reason
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM orders WHERE order_id = ANY(ARRAY[${uuid.list(n)}])".command

  private val getOrdersQuery: Query[Void, OrderView] =
    sql"SELECT $columns FROM orders ORDER BY order_id".query(orderViewCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): OrderRepository[F] =
    new OrderRepository(pool)
