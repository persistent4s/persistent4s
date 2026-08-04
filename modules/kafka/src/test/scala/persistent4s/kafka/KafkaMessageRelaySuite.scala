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

package persistent4s.kafka

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import weaver.SimpleIOSuite

import persistent4s.{MessageOutbox, MessagePublisher, OutgoingMessage}

object KafkaMessageRelaySuite extends SimpleIOSuite:

  /** Serves `pending` in FIFO batches; a batch is dropped only after `publish` succeeds (mirrors drainBatch's
    * rollback).
    */
  final private class FakeMessageOutbox(pending: Ref[IO, List[(Long, OutgoingMessage)]]) extends MessageOutbox[IO]:

    override def enqueue(messages: List[OutgoingMessage]): IO[Unit] = IO.unit

    override def drainBatch(batchSize: Int)(publish: List[(Long, OutgoingMessage)] => IO[Unit]): IO[Int] =
      pending.get.flatMap { all =>
        val batch = all.take(batchSize)
        if batch.isEmpty then IO.pure(0)
        else publish(batch) *> pending.update(_.drop(batch.size)).as(batch.size)
      }

    override def notifications: Stream[IO, Unit] = Stream.empty

  final private class FakeMessagePublisher(
    recorded: Ref[IO, Vector[OutgoingMessage]],
    fail: Boolean = false,
  ) extends MessagePublisher[IO]:

    override def publish(message: OutgoingMessage): IO[Unit] =
      if fail then IO.raiseError(new RuntimeException("boom")) else recorded.update(_ :+ message)

    override def publish(messages: List[OutgoingMessage]): IO[Unit] =
      if fail then IO.raiseError(new RuntimeException("boom")) else recorded.update(_ ++ messages)

  private def waitFor[A](ref: Ref[IO, Vector[A]], n: Int, timeout: FiniteDuration = 5.seconds): IO[Unit] =
    def poll: IO[Unit] = ref.get.flatMap(v => if v.size >= n then IO.unit else IO.sleep(20.millis) *> poll)
    poll.timeoutTo(timeout, IO.unit)

  private def entry(id: Long, payload: String): (Long, OutgoingMessage) = (id, OutgoingMessage("t", None, payload))

  test("publishes every message drained from the outbox") {
    for
      pending  <- IO.ref(List(entry(1L, "a"), entry(2L, "b"), entry(3L, "c")))
      recorded <- IO.ref(Vector.empty[OutgoingMessage])
      relay     = KafkaMessageRelay[IO](
                new FakeMessageOutbox(pending),
                new FakeMessagePublisher(recorded),
                batchSize = 10,
                pollInterval = 10.millis,
              )
      _    <- relay.runOnce.background.use(_ => waitFor(recorded, 3))
      pubs <- recorded.get
    yield expect(pubs.map(_.payload) == Vector("a", "b", "c"))
  }

  test("a publish failure fails the run and leaves the batch undrained") {
    for
      pending  <- IO.ref(List(entry(1L, "a")))
      recorded <- IO.ref(Vector.empty[OutgoingMessage])
      relay     = KafkaMessageRelay[IO](
                new FakeMessageOutbox(pending),
                new FakeMessagePublisher(recorded, fail = true),
                batchSize = 10,
                pollInterval = 1.hour,
              )
      result    <- relay.runOnce.attempt
      remaining <- pending.get
    yield expect.all(result.isLeft, remaining.size == 1)
  }
