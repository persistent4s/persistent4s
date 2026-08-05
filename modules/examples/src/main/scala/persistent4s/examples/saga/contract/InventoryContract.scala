package persistent4s.examples.saga.contract

import java.util.UUID
import io.circe.Encoder
import io.circe.Decoder
import persistent4s.MessageCodec
import persistent4s.circe.CirceMessageCodec

final case class ReserveStock(orderId: UUID, itemId: UUID, amount: Int) derives Encoder.AsObject, Decoder

object ReserveStock:

  val Kind = "reserve"

  given MessageCodec[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

final case class ReleaseStock(orderId: UUID, itemId: UUID) derives Encoder.AsObject, Decoder

object ReleaseStock:

  val Kind = "release"

  given MessageCodec[ReleaseStock] = CirceMessageCodec.derived[ReleaseStock]
