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

package persistent4s.examples.saga.orders.saga

import persistent4s.{MessageCodec, MessageEncoder}
import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment, ReleaseStock, ReserveStock}

type OrderRequest = ReserveStock | ReleaseStock | AuthorizePayment | CancelPayment

object OrderRequest:

  val encoder: MessageEncoder[OrderRequest] = new MessageEncoder[OrderRequest]:

    def encode(request: OrderRequest): Either[Throwable, String] = request match
      case r: ReserveStock     => summon[MessageCodec[ReserveStock]].encode(r)
      case r: ReleaseStock     => summon[MessageCodec[ReleaseStock]].encode(r)
      case a: AuthorizePayment => summon[MessageCodec[AuthorizePayment]].encode(a)
      case c: CancelPayment    => summon[MessageCodec[CancelPayment]].encode(c)
