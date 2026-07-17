package persistent4s

import cats.effect.IO
import cats.effect.Ref
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import weaver.SimpleIOSuite

object InstrumentedProjectionCheckpointSuite extends SimpleIOSuite:

  given Tracer[IO] = Tracer.Implicits.noop

  given Meter[IO] = Meter.Implicits.noop

  final class FakeCheckpoint(ref: Ref[IO, Map[String, ProjectionCheckpointState]]) extends ProjectionCheckpoint[IO]:

    def load(projectionName: String): IO[Option[ProjectionCheckpointState]] =
      ref.get.map(_.get(projectionName))

    def save(state: ProjectionCheckpointState): IO[Unit] =
      ref.update(_.updated(state.projectionName, state))

    def loadAll(): IO[List[ProjectionCheckpointState]] =
      ref.get.map(_.values.toList)

  final class FailingCheckpoint extends ProjectionCheckpoint[IO]:

    def load(projectionName: String): IO[Option[ProjectionCheckpointState]] =
      IO.raiseError(new RuntimeException("boom"))

    def save(state: ProjectionCheckpointState): IO[Unit] =
      IO.raiseError(new RuntimeException("boom"))

    def loadAll(): IO[List[ProjectionCheckpointState]] =
      IO.raiseError(new RuntimeException("boom"))

  test("save delegates to the inner checkpoint") {
    for
      ref          <- Ref.of[IO, Map[String, ProjectionCheckpointState]](Map.empty)
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](FakeCheckpoint(ref))
      state         = ProjectionCheckpointState("proj-a", 5L, running = true, None)
      _            <- instrumented.save(state)
      stored       <- ref.get
    yield expect(stored.get("proj-a").contains(state))
  }

  test("load delegates to the inner checkpoint") {
    for
      ref          <- Ref.of[IO, Map[String, ProjectionCheckpointState]](Map.empty)
      inner         = FakeCheckpoint(ref)
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](inner)
      state         = ProjectionCheckpointState("proj-b", 3L, running = true, None)
      _            <- inner.save(state)
      loaded       <- instrumented.load("proj-b")
    yield expect(loaded.contains(state))
  }

  test("load returns None when no checkpoint exists for the projection") {
    for
      ref          <- Ref.of[IO, Map[String, ProjectionCheckpointState]](Map.empty)
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](FakeCheckpoint(ref))
      loaded       <- instrumented.load("missing")
    yield expect(loaded.isEmpty)
  }

  test("loadAll delegates to the inner checkpoint") {
    for
      ref          <- Ref.of[IO, Map[String, ProjectionCheckpointState]](Map.empty)
      inner         = FakeCheckpoint(ref)
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](inner)
      stateA        = ProjectionCheckpointState("proj-a", 1L, running = true, None)
      stateB        = ProjectionCheckpointState("proj-b", 2L, running = false, Some("err"))
      _            <- inner.save(stateA) *> inner.save(stateB)
      all          <- instrumented.loadAll()
    yield expect(all.toSet == Set(stateA, stateB))
  }

  test("save propagates an inner failure") {
    for
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](new FailingCheckpoint)
      result       <- instrumented.save(ProjectionCheckpointState("proj-c", 0L, running = true, None)).attempt
    yield expect(result.isLeft)
  }

  test("load propagates an inner failure") {
    for
      instrumented <- InstrumentedProjectionCheckpoint.make[IO](new FailingCheckpoint)
      result       <- instrumented.load("proj-c").attempt
    yield expect(result.isLeft)
  }
