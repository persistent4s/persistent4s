package persistent4s

import java.time.Instant

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger
import scala.util.Try

/** A saga request as it reaches the partner, with the moment it was picked up.
  *
  * The clock is read once, here, and travels as data: a handler judging whether a request is too old to honour needs
  * `receivedAt`, and taking it as a value is what keeps the function that judges it pure.
  */
final case class RequestContext(message: IncomingMessage, receivedAt: Instant):

  /** Whether this request nominates somewhere to answer. `false` for a command sent by something that is not a saga,
    * which is legitimate — it just means nobody is waiting, and the handler's reply, if it builds one, goes nowhere.
    */
  def isAddressed: Boolean =
    List(SagaHeaders.ReplyTo, SagaHeaders.Name, SagaHeaders.Id).forall(message.headers.contains)

  /** Whether the caller's stated deadline has passed.
    *
    * No header means no expiry: a caller that never set one gets the plain fire-and-forget behaviour, which is right
    * for a command nobody is waiting on.
    */
  def hasExpired: Boolean =
    message.headers.get(SagaHeaders.ExpiresAt) match
      case None        => false
      case Some(value) => Try(Instant.parse(value)).toOption.forall(receivedAt.isAfter)

/** Consumes a topic of saga requests and routes each to the handler registered for its type.
  *
  * Everything here is the same in every partner: subscribe, read the discriminator, decode, note when the request is
  * unaddressed, acknowledge. Only the handlers differ, so only the handlers are yours to write.
  *
  * Acknowledgement follows the same rule as [[SagaRunner]]'s reply loop. A message this cannot route or decode is a
  * permanent failure — a redelivery would fail identically — so it is logged and acked rather than left to block the
  * partition. Anything a handler raises propagates: the message stays unacked and the broker redelivers it.
  */
final class SagaParticipant[F[_]: Async: Logger] private (
  routes: Vector[SagaParticipant.Route[F]],
):

  /** Register the handler for one type of request. The first registration for a type wins.
    *
    * Which requests it claims comes from `C`'s own [[RequestType]] — the same declaration the sender stamped them with,
    * so there is no name to keep in step here.
    */
  def on[C](handle: (RequestContext, C) => F[Unit])(using
    requestType: RequestType[C],
    decoder: MessageDecoder[C],
  ): SagaParticipant[F] =
    val name = requestType.name
    val route = SagaParticipant.Route[F](
      name,
      ctx =>
        ctx.message.as[C] match
          case Right(command) => handle(ctx, command)
          case Left(error)    =>
            Logger[F].error(error)(
              s"could not decode a '$name' request from '${ctx.message.topic}', dropping it: ${ctx.message.payload}",
            ),
    )
    new SagaParticipant[F](routes :+ route)

  /** Register a handler that answers, for a partner with no log of its own.
    *
    * The reply goes out as soon as the handler returns, which is only safe when there is nothing to make it atomic
    * with — a stateless authorization, a lookup against something outside this service. A partner that appends events
    * has to enqueue its reply in the transaction that appends them, and that is [[SagaCommandHandler]]'s job.
    *
    * An unencodable reply is logged and dropped rather than raised, unlike in [[SagaCommandHandler]]. The difference is
    * the atomicity: there, failing to reply has to undo the append too, so raising is the only safe answer; here
    * nothing has been written, and blocking the partition on a failure that will repeat identically costs more than
    * letting the asking saga reach its deadline.
    */
  def replying[C, A](publisher: MessagePublisher[F])(handle: (RequestContext, C) => F[A])(using
    requestType: RequestType[C],
    decoder: MessageDecoder[C],
    encoder: MessageEncoder[A],
  ): SagaParticipant[F] =
    on[C] { (ctx, command) =>
      handle(ctx, command).flatMap { answer =>
        encoder.encode(answer) match
          case Left(error)    =>
            Logger[F].error(error)(s"could not encode the reply to a '${requestType.name}' request, dropping it")
          case Right(payload) =>
            // `None` means the request nominated nowhere to answer, which `dispatch` has already warned about.
            SagaHeaders.reply(ctx.message, payload).traverse_(publisher.publish)
      }
    }

  def subscribe(subscriber: MessageSubscriber[F], topic: String, fromBeginning: Boolean = true): Stream[F, Unit] =
    Stream.eval(checkRoutes(topic)) ++
      subscriber.subscribe(topic, fromBeginning).evalMap { case (message, ack) => dispatch(message) *> ack }

  /** Reported once, when the stream starts, because both mistakes are silent otherwise: a participant with no routes
    * drops everything it is sent, and two handlers for one request type means one of them will never run.
    */
  private def checkRoutes(topic: String): F[Unit] =
    val duplicated = routes.groupBy(_.requestType).collect { case (name, rs) if rs.sizeIs > 1 => name }
    Async[F].whenA(routes.isEmpty)(
      Logger[F].warn(s"no handlers registered for '$topic'; every request will be dropped"),
    ) *>
      Async[F].whenA(duplicated.nonEmpty)(
        Logger[F].warn(s"'$topic' has more than one handler for ${duplicated.mkString(", ")}; the first one wins"),
      )

  private def dispatch(message: IncomingMessage): F[Unit] =
    message.headers.get(SagaHeaders.RequestType) match
      case None =>
        Logger[F].error(
          s"request from '${message.topic}' carries no '${SagaHeaders.RequestType}' header, dropping it",
        )
      case Some(name) =>
        routes.find(_.requestType == name) match
          case None =>
            Logger[F].error(s"no handler for '$name' requests from '${message.topic}', dropping it: ${message.payload}")
          case Some(route) =>
            for
              now <- Clock[F].realTimeInstant
              ctx  = RequestContext(message, now)
              _   <- Async[F].unlessA(ctx.isAddressed)(
                     Logger[F].warn(
                       s"a '$name' request is not fully addressed (needs ${SagaHeaders.ReplyTo}, " +
                         s"${SagaHeaders.Name} and ${SagaHeaders.Id}); honouring it, but nobody will be answered",
                     ),
                   )
              _ <- route.run(ctx)
            yield ()

object SagaParticipant:

  final private case class Route[F[_]](requestType: String, run: RequestContext => F[Unit])

  def apply[F[_]: Async: Logger]: SagaParticipant[F] = new SagaParticipant[F](Vector.empty)
