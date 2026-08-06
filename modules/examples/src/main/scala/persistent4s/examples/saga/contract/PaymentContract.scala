package persistent4s.examples.saga.contract

import io.circe.Encoder
import io.circe.Decoder
import persistent4s.MessageCodec
import persistent4s.RequestType
import persistent4s.circe.CirceMessageCodec
import java.util.UUID

final case class AuthorizePayment(orderId: UUID, customerId: UUID, price: Int) derives Encoder.AsObject, Decoder

object AuthorizePayment:

  given RequestType[AuthorizePayment] = RequestType("authorize")

  given MessageCodec[AuthorizePayment] = CirceMessageCodec.derived[AuthorizePayment]

final case class CancelPayment(orderId: UUID, customerId: UUID) derives Encoder.AsObject, Decoder

object CancelPayment:

  given RequestType[CancelPayment] = RequestType("cancel")

  given MessageCodec[CancelPayment] = CirceMessageCodec.derived[CancelPayment]
