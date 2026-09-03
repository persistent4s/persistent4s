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

import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import weaver.IOSuite

import persistent4s.{IncomingMessage, OutgoingMessage}

/** Integration tests for [[KafkaModule.messageSubscriber]] against a real broker via testcontainers.
  *
  * Messages are published with `KafkaModule.messagePublisher` and read back with `KafkaModule.messageSubscriber`, so
  * the pair is exercised the way it is used in production: a relay drains the outbox, a partner service consumes.
  * Topics and consumer groups are unique per test so the shared broker cannot leak state across them.
  */
object KafkaMessageSubscriberSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = KafkaContainer

  override def sharedResource: Resource[IO, KafkaContainer] =
    Resource.make(
      IO.blocking {
        val c = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))
        c.start()
        c
      },
    )(c => IO.blocking(c.stop()).handleErrorWith(_ => IO.unit))

  private def producerConfig(container: KafkaContainer): KafkaMessageProducerConfig =
    KafkaMessageProducerConfig(bootstrapServers = container.getBootstrapServers)

  private def consumerConfig(container: KafkaContainer, groupId: String): KafkaConsumerConfig =
    KafkaConsumerConfig(bootstrapServers = container.getBootstrapServers, groupId = groupId)

  private def uniqueId(label: String): String = s"$label-${UUID.randomUUID()}"

  /** Start the subscription, give the broker time to assign partitions, then publish. Publishing from a concurrent
    * fiber rather than up front is what makes these tests independent of assignment timing.
    *
    * The acknowledge actions are deliberately dropped: the stream ends with `take`, which closes the consumer, so an
    * ack is only valid while the stream is still running. Tests that care about acking do it inside the stream.
    */
  private def publishThenConsume(
    container: KafkaContainer,
    topic: String,
    group: String,
    messages: List[OutgoingMessage],
    take: Int,
    fromBeginning: Boolean = true,
  ): IO[List[IncomingMessage]] =
    (
      KafkaModule.messagePublisher[IO](producerConfig(container)),
      KafkaModule.messageSubscriber[IO](consumerConfig(container, group)),
    ).tupled.use { case (publisher, subscriber) =>
      val consume = subscriber.subscribe(topic, fromBeginning).take(take).compile.toList.timeout(30.seconds)
      IO.both(consume, IO.sleep(3.seconds) *> publisher.publish(messages)).map(_._1.map(_._1))
    }

  /** Read `take` messages in a fresh session on an existing consumer group, resuming from its committed offset. */
  private def resume(container: KafkaContainer, topic: String, group: String, take: Int): IO[List[IncomingMessage]] =
    KafkaModule
      .messageSubscriber[IO](consumerConfig(container, group))
      .use(
        _.subscribe(topic, fromBeginning = true).take(take).compile.toList.timeout(30.seconds).map(_.map(_._1)),
      )

  test("a message round-trips with topic, key, payload and headers preserved") { container =>
    val topic = uniqueId("round-trip")
    val sent = OutgoingMessage(
      topic = topic,
      key = Some("student-42"),
      payload = """{"courseId":"c-1"}""",
      headers = Map("persistent4s.sagaName" -> "reserve-seat", "persistent4s.sagaId" -> "s-1"),
    )

    publishThenConsume(container, topic, uniqueId("group"), List(sent), take = 1).map { received =>
      expect.all(
        received.size == 1,
        received.head.topic == topic,
        received.head.key == Some("student-42"),
        received.head.payload == sent.payload,
        received.head.headers == sent.headers,
      )
    }
  }

  test("a keyless message arrives with key = None") { container =>
    val topic = uniqueId("keyless")
    val sent = OutgoingMessage(topic = topic, key = None, payload = "body")

    publishThenConsume(container, topic, uniqueId("group"), List(sent), take = 1).map { received =>
      expect(received.head.key.isEmpty)
    }
  }

  test("a message with no headers arrives with an empty header map") { container =>
    val topic = uniqueId("no-headers")
    val sent = OutgoingMessage(topic = topic, key = Some("k"), payload = "body")

    publishThenConsume(container, topic, uniqueId("group"), List(sent), take = 1).map { received =>
      expect(received.head.headers.isEmpty)
    }
  }

  test("acknowledging commits the offset, so a later session resumes after it") { container =>
    val topic = uniqueId("ack")
    val group = uniqueId("group")
    val first = OutgoingMessage(topic, Some("k"), "first")
    val second = OutgoingMessage(topic, Some("k"), "second")

    val readAndAck =
      (
        KafkaModule.messagePublisher[IO](producerConfig(container)),
        KafkaModule.messageSubscriber[IO](consumerConfig(container, group)),
      ).tupled.use { case (publisher, subscriber) =>
        val consume = subscriber
          .subscribe(topic, fromBeginning = true)
          .evalMap { case (message, ack) => ack.as(message) }
          .take(1)
          .compile
          .toList
          .timeout(30.seconds)
        IO.both(consume, IO.sleep(3.seconds) *> publisher.publish(List(first, second))).map(_._1)
      }

    for
      acked   <- readAndAck
      resumed <- resume(container, topic, group, take = 1)
    yield expect.all(acked.head.payload == "first", resumed.head.payload == "second")
  }

  test("not acknowledging replays the message in a later session") { container =>
    val topic = uniqueId("no-ack")
    val group = uniqueId("group")
    val sent = OutgoingMessage(topic, Some("k"), "only")

    for
      received <- publishThenConsume(container, topic, group, List(sent), take = 1)
      replayed <- resume(container, topic, group, take = 1)
    yield expect.all(received.head.payload == "only", replayed.head.payload == "only")
  }

  test("fromBeginning = false skips records published before the subscription") { container =>
    val topic = uniqueId("latest")
    val before = OutgoingMessage(topic, Some("k"), "before")
    val after = OutgoingMessage(topic, Some("k"), "after")

    for
      _        <- KafkaModule.messagePublisher[IO](producerConfig(container)).use(_.publish(before))
      received <- publishThenConsume(
                    container, topic, uniqueId("group"), List(after), take = 1, fromBeginning = false,
                  )
    yield expect(received.head.payload == "after")
  }
