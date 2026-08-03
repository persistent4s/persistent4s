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

package persistent4s.examples.saga.orders.infrastructure

import java.time.Instant
import java.util.UUID

import cats.effect.IO
import cats.syntax.all.*
import io.circe.{Decoder, Encoder, Json}
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*

import persistent4s.{EventStore, SagaId, SagaRecord}
import persistent4s.examples.saga.docs.SwaggerRoutes
import persistent4s.examples.saga.orders.domain.OrdersEvent
import persistent4s.examples.saga.orders.domain.customer.{RegisterCustomer, RegisterCustomerHandler}
import persistent4s.examples.saga.orders.domain.order.{PlaceOrder, PlaceOrderHandler}
import persistent4s.examples.saga.orders.saga.ReserveStockSaga

final case class RegisterCustomerRequest(customerId: UUID, name: String) derives Decoder

final case class PlaceOrderRequest(orderId: UUID, customerId: UUID, itemId: UUID, amount: Int) derives Decoder

/** What `POST /orders` can honestly promise: the order was accepted, and its outcome is somewhere else.
  *
  * Not read from the projection — a read immediately after the append can lose the race with the projector and would then
  * have to answer 404 for an order that certainly exists. This is the outcome of the request itself; the outcome of the
  * ''order'' has to be polled, which is the whole point.
  */
final case class OrderAccepted(orderId: UUID, status: String, outcomeAt: String) derives Encoder.AsObject

final case class ErrorResponse(error: String) derives Encoder.AsObject

object OrdersRoutes:

  def make(module: OrdersModule): HttpRoutes[IO] =
    api(module) <+> SwaggerRoutes.routes("saga/orders-openapi.yaml", "Orders service")

  private def api(module: OrdersModule): HttpRoutes[IO] =
    given EventStore[IO, OrdersEvent] = module.store

    HttpRoutes.of[IO] {
      case request @ POST -> Root / "customers" =>
        for
          body     <- request.as[RegisterCustomerRequest]
          result   <- RegisterCustomerHandler.run[IO](RegisterCustomer(body.customerId, body.name)).attempt
          response <- result match
                        case Left(error) => BadRequest(ErrorResponse(error.getMessage))
                        case Right(_)    => Created(Json.obj("customerId" -> Json.fromString(body.customerId.toString)))
        yield response

      // 202, not 201: everything this service can decide has been decided, and that is not enough to say the order will
      // be honoured.
      case request @ POST -> Root / "orders" =>
        for
          body     <- request.as[PlaceOrderRequest]
          command   = PlaceOrder(body.orderId, body.customerId, body.itemId, body.amount)
          result   <- PlaceOrderHandler.run[IO](command).attempt
          response <- result match
                        case Left(error) => BadRequest(ErrorResponse(error.getMessage))
                        case Right(_)    =>
                          Accepted(OrderAccepted(body.orderId, "Placed", s"/orders/${body.orderId}"))
        yield response

      case GET -> Root / "orders" =>
        module.orderRepository.getOrders.flatMap(Ok(_))

      case GET -> Root / "orders" / UUIDVar(orderId) =>
        module.orderRepository.find(orderId).flatMap {
          case Some(view) => Ok(view)
          case None       => NotFound(ErrorResponse(s"No such order: $orderId"))
        }

      // The instance behind an order, for poking at the machinery. The id is not stored anywhere on the order: it is
      // derived from the saga's name and key, which is exactly how replaying a trigger finds the same row again.
      case GET -> Root / "orders" / UUIDVar(orderId) / "saga" =>
        module.sagaRepository.find(SagaId.instance(ReserveStockSaga.name, orderId.toString)).flatMap {
          case Some(record) => Ok(sagaJson(record))
          case None         => NotFound(ErrorResponse(s"No saga instance for order: $orderId"))
        }
    }

  /** Hand-written because [[SagaRecord]] is a library type with no circe instance — and it is the runner's bookkeeping,
    * not part of anyone's API, so it should not have one.
    */
  private def sagaJson(record: SagaRecord): Json =
    Json.obj(
      "id"        -> Json.fromString(record.id.toString),
      "sagaName"  -> Json.fromString(record.sagaName),
      "key"       -> Json.fromString(record.key),
      "status"    -> Json.fromString(record.status.toString),
      "step"      -> Json.fromInt(record.step),
      "data"      -> Json.fromString(record.data),
      "deadline"  -> record.deadline.fold(Json.Null)(instantJson),
      "createdAt" -> instantJson(record.createdAt),
      "updatedAt" -> instantJson(record.updatedAt),
    )

  private def instantJson(instant: Instant): Json = Json.fromString(instant.toString)
