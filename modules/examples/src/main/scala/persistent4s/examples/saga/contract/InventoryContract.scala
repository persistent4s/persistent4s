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

package persistent4s.examples.saga.contract

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import persistent4s.MessageCodec
import persistent4s.RequestType
import persistent4s.circe.CirceMessageCodec

final case class ReserveStock(orderId: UUID, itemId: UUID, amount: Int) derives Encoder.AsObject, Decoder

object ReserveStock:

  /** The name inventory knows this request by. Declared once, here in the contract both services share: the saga stamps
    * it and inventory routes on it, neither having written it down a second time.
    */
  given RequestType[ReserveStock] = RequestType("reserve")

  given MessageCodec[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

final case class ReleaseStock(orderId: UUID, itemId: UUID) derives Encoder.AsObject, Decoder

object ReleaseStock:

  given RequestType[ReleaseStock] = RequestType("release")

  given MessageCodec[ReleaseStock] = CirceMessageCodec.derived[ReleaseStock]
