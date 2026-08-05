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

/** Everything the orders service can ask a partner to do.
  *
  * A '''union''' rather than a sealed trait, for two reasons. A sealed hierarchy would need all four DTOs in one file,
  * collapsing the per-partner contracts into a single shared one — and it would imply the partners share a request
  * vocabulary, when in truth inventory has never heard of `AuthorizePayment`. And this type belongs here rather than in
  * `contract`: it is the ''caller's'' view of what it can ask for, so no partner needs it.
  *
  * Exhaustiveness is still checked, so adding a fifth request to the union will not compile until [[OrderRequest.encoder]]
  * knows how to write it.
  */
type OrderRequest = ReserveStock | ReleaseStock | AuthorizePayment | CancelPayment

object OrderRequest:

  /** Dispatches to each DTO's own codec, so what goes on the wire is the bare shape the receiving partner decodes — not
    * the `{"ReserveStock": {...}}` wrapper that deriving one codec for a sealed hierarchy would have produced.
    *
    * Encode-only on purpose: there is no honest `decode` here. An incoming payload could be any of the four and nothing
    * in it says which, which is why [[persistent4s.Saga.requestEncoder]] asks for a [[MessageEncoder]] rather than a
    * codec.
    *
    * A plain `val`, not a `given`: implicit search never looks in the companion of a type ''alias'', so a `given` here
    * would be invisible to `summon[MessageEncoder[OrderRequest]]` and the saga would fail to find its own encoder. Name
    * it at the one place it is used instead.
    */
  val encoder: MessageEncoder[OrderRequest] = new MessageEncoder[OrderRequest]:

    def encode(request: OrderRequest): Either[Throwable, String] = request match
      case r: ReserveStock     => summon[MessageCodec[ReserveStock]].encode(r)
      case r: ReleaseStock     => summon[MessageCodec[ReleaseStock]].encode(r)
      case a: AuthorizePayment => summon[MessageCodec[AuthorizePayment]].encode(a)
      case c: CancelPayment    => summon[MessageCodec[CancelPayment]].encode(c)
