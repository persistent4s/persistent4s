package persistent4s

import cats.Applicative
import cats.syntax.all.*

trait StatelessProjection[F[_]: Applicative, A <: Event] extends Projection[F, A, scala.Unit] {

  type State = scala.Unit

  override def resolveKeys(event: EventEnvelope[A]): List[Unit] = List(())

  override def fetchState(key: Unit): F[Option[State]] = Applicative[F].pure(None)

  def handle(event: EventEnvelope[A]): F[Unit]

  final def handle(state: Option[Unit], event: EventEnvelope[A]): F[Option[Unit]] =
    handle(event).as(state)

  override def persist(key: Unit, state: Option[Unit]): F[Unit] = Applicative[F].unit

}
