package persistent4s

import cats.Applicative

trait StatelessProjection[F[_]: Applicative, A <: Event] extends Projection[F, A, scala.Unit] {

  type State = scala.Unit

  override def initialState: State = ()

  override def resolveKeys(event: EventEnvelope[A]): List[Unit] = List(())

  override def fetchState(key: Unit): F[Option[State]] = Applicative[F].pure(None)

  def handle(event: EventEnvelope[A]): F[Unit]

  final def handle(state: Unit, event: EventEnvelope[A]): F[Unit] = handle(event)

  override def persist(key: Unit, state: Unit): F[Unit] = Applicative[F].unit

}
