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

package persistent4s.examples.saga.inventory.infrastructure

import java.util.UUID

import cats.effect.IO
import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.io.*

import persistent4s.{CommandHandlerMetrics, EventFilter, EventStore}
import persistent4s.examples.saga.docs.SwaggerRoutes
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags}
import persistent4s.examples.saga.inventory.domain.item.{ItemStock, RestockItem, RestockItemHandler}

final case class RestockRequest(amount: Int) derives Decoder

final case class ErrorResponse(error: String) derives Encoder.AsObject

/** Two routes, written by hand. There is no route for reserving stock: the only way to ask for a reservation is to send
  * the command, because the saga is the caller this service was built for.
  */
object InventoryRoutes:

  def make(module: InventoryModule): HttpRoutes[IO] =
    api(module) <+> SwaggerRoutes.routes("saga/inventory-openapi.yaml", "Inventory service")

  private def api(module: InventoryModule): HttpRoutes[IO] =
    given EventStore[IO, InventoryEvent] = module.store

    given CommandHandlerMetrics[IO] = module.commandMetrics

    HttpRoutes.of[IO] {
      case request @ POST -> Root / "items" / UUIDVar(itemId) / "restock" =>
        for
          body     <- request.as[RestockRequest]
          result   <- RestockItemHandler.run[IO](RestockItem(itemId, body.amount)).attempt
          response <- result match
                        case Left(error) => BadRequest(ErrorResponse(error.getMessage))
                        case Right(_)    => readStock(module, itemId).flatMap(Ok(_))
        yield response

      case GET -> Root / "items" / UUIDVar(itemId) =>
        readStock(module, itemId).flatMap(Ok(_))
    }

  private def readStock(module: InventoryModule, itemId: UUID): IO[ItemStock] =
    module.store
      .readFrom(0L, EventFilter(tags = Set(InventoryTags.item(itemId))))
      .compile
      .toList
      .map(envelopes => ItemStock.fold(itemId, envelopes.map(_.payload)))
