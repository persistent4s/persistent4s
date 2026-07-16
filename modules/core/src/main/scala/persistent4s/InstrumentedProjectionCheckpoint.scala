package persistent4s

import cats.effect.Async
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Histogram, Meter}
import org.typelevel.otel4s.trace.Tracer

/** Decorates any [[ProjectionCheckpoint]] with otel4s spans and a duration histogram.
  *
  * Emits:
  *   - span `persistent4s.checkpoint.save` per save call
  *   - span `persistent4s.checkpoint.load` per load call
  *   - span `persistent4s.checkpoint.load_all` per loadAll call
  *   - histogram `persistent4s.checkpoint.save.duration` (ms)
  */
final class InstrumentedProjectionCheckpoint[F[_]: Async: Tracer] private (
  inner: ProjectionCheckpoint[F],
  saveDuration: Histogram[F, Double],
) extends ProjectionCheckpoint[F]:

  override def load(projectionName: String): F[Option[ProjectionCheckpointState]] =
    Tracer[F]
      .spanBuilder("persistent4s.checkpoint.load")
      .addAttribute(Attribute("projection.name", projectionName))
      .build
      .surround(inner.load(projectionName))

  override def save(state: ProjectionCheckpointState): F[Unit] =
    val projAttr = Attribute("projection.name", state.projectionName)
    Tracer[F]
      .spanBuilder("persistent4s.checkpoint.save")
      .addAttribute(projAttr)
      .build
      .surround(
        for
          start <- Async[F].monotonic
          _     <- inner.save(state)
          end   <- Async[F].monotonic
          _     <- saveDuration.record((end - start).toNanos.toDouble / 1e6, projAttr)
        yield (),
      )

  override def loadAll(): F[List[ProjectionCheckpointState]] =
    Tracer[F].spanBuilder("persistent4s.checkpoint.load_all").build.surround(inner.loadAll())

object InstrumentedProjectionCheckpoint:

  def make[F[_]: Async: Tracer: Meter](inner: ProjectionCheckpoint[F]): F[InstrumentedProjectionCheckpoint[F]] =
    Meter[F]
      .histogram[Double]("persistent4s.checkpoint.save.duration")
      .withDescription("Time to persist a projection checkpoint")
      .withUnit("ms")
      .create
      .map(new InstrumentedProjectionCheckpoint(inner, _))
