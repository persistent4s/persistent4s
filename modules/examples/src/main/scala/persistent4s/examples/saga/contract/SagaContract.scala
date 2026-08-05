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

import io.circe.{Decoder, Encoder}

import persistent4s.MessageCodec
import persistent4s.circe.CirceMessageCodec

final case class PartnerReply(accepted: Boolean, reason: Option[String]) derives Encoder.AsObject, Decoder

object PartnerReply:

  val accept: PartnerReply = PartnerReply(accepted = true, reason = None)

  def reject(reason: String): PartnerReply = PartnerReply(accepted = false, reason = Some(reason))

  given MessageCodec[PartnerReply] = CirceMessageCodec.derived[PartnerReply]

/** Headers the three services agree on, beyond the reserved `persistent4s.*` ones the runner stamps itself. */
object RequestHeaders:

  val Kind = "kind"

  val ExpiresAt = "expiresAt"

/** Topics services have to agree on. */
object Topics:

  val InventoryCommands = "inventory.commands"

  val PaymentCommands = "payment.commands"

  val OrdersReplies = "orders.replies"
