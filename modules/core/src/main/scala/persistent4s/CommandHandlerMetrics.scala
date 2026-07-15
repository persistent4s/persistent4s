package persistent4s

import cats.Functor
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Counter, Meter}

/** Metrics used by [[CommandHandler.run]]. Build once per application (e.g. alongside the event store) and provide as a
  * given. The retries counter must not be recreated on every command.
  */
final case class CommandHandlerMetrics[F[_]](retries: Counter[F, Long])

object CommandHandlerMetrics:

  def make[F[_]: Meter: Functor]: F[CommandHandlerMetrics[F]] =
    Meter[F]
      .counter[Long]("persistent4s.commandhandler.retries")
      .withDescription("Number of command handler retry attempts on confilct")
      .withUnit("{retries}")
      .create
      .map(CommandHandlerMetrics(_))
