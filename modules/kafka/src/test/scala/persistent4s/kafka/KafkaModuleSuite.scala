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

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import weaver.IOSuite

import persistent4s.{Event, EventEnvelope, EventMetadata, EventTypeName, Tag}
import persistent4s.circe.CirceEventCodec

/** Integration tests for [[KafkaModule]] using a real Kafka instance via testcontainers.
  *
  * Each test publishes events via the publisher built from `KafkaModule.publisher` and consumes them with the
  * subscriber built from `KafkaModule.subscriber`, verifying the round-trip preserves both metadata and payload. Topics
  * and consumer groups are unique per test to avoid cross-test interference on the shared broker.
  */
object KafkaModuleSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = KafkaContainer

  final case class TestEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val codec = CirceEventCodec.make[TestEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[TestEvent].left.map(identity),
  )

  override def sharedResource: Resource[IO, KafkaContainer] =
    Resource.make(
      IO.blocking {
        val c = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"))
        c.start()
        c
      },
    )(c => IO.blocking(c.stop()).handleErrorWith(_ => IO.unit))

  /** Build a producer config that constant-keys all records (one-partition ordering scenario). */
  private def producerConfig(container: KafkaContainer): KafkaProducerConfig[TestEvent] =
    KafkaProducerConfig(
      bootstrapServers = container.getBootstrapServers,
      recordKey = _ => "test-service",
    )

  private def consumerConfig(container: KafkaContainer, groupId: String): KafkaConsumerConfig =
    KafkaConsumerConfig(
      bootstrapServers = container.getBootstrapServers,
      groupId = groupId,
    )

  private def envelope(globalPosition: Long, value: String, tags: Set[Tag] = Set.empty): EventEnvelope[TestEvent] =
    EventEnvelope(
      EventMetadata(
        globalPosition = globalPosition, id = UUID.randomUUID(), tags = tags,
        eventType = EventTypeName.fromString("TestEvent"), isExternal = false,
        timestamp = Instant.parse("2026-01-01T00:00:00Z"), headers = Map.empty,
      ),
      TestEvent(value),
    )

  private def uniqueId(label: String): String = s"$label-${UUID.randomUUID()}"

  test("single envelope round-trips with metadata preserved") { container =>
    val topic = uniqueId("round-trip")
    val group = uniqueId("group")
    val expected = envelope(1L, "hello", tags = Set(Tag.fromString("user:1").get))

    val program =
      for received <- (
                        KafkaModule.publisher[IO, TestEvent](producerConfig(container), codec),
                        KafkaModule.subscriber[IO, TestEvent](consumerConfig(container, group), codec),
                      ).tupled.use { case (publisher, subscriber) =>
                        val consume = subscriber
                          .subscribe(topic, fromBeginning = true)
                          .take(1)
                          .compile
                          .toList
                          .timeout(30.seconds)
                        IO.both(consume, IO.sleep(2.seconds) *> publisher.publish(topic, expected)).map(_._1)
                      }
      yield received

    program.map { records =>
      val (got, _offset) = records.head
      expect.all(
        records.size == 1,
        got.payload == expected.payload,
        got.metadata.globalPosition == expected.metadata.globalPosition,
        got.metadata.id == expected.metadata.id,
        got.metadata.eventType == expected.metadata.eventType,
        got.metadata.tags == expected.metadata.tags,
        got.metadata.timestamp == expected.metadata.timestamp,
      )
    }
  }

  test("batch publish delivers all envelopes in order") { container =>
    val topic = uniqueId("batch")
    val group = uniqueId("group")
    val expected = List(envelope(1L, "a"), envelope(2L, "b"), envelope(3L, "c"))

    val program = (
      KafkaModule.publisher[IO, TestEvent](producerConfig(container), codec),
      KafkaModule.subscriber[IO, TestEvent](consumerConfig(container, group), codec),
    ).tupled.use { case (publisher, subscriber) =>
      val consume = subscriber
        .subscribe(topic, fromBeginning = true)
        .take(expected.size.toLong)
        .compile
        .toList
        .timeout(30.seconds)
      IO.both(consume, IO.sleep(2.seconds) *> publisher.publish(topic, expected)).map(_._1)
    }

    program.map { records =>
      val payloads = records.map(_._1.payload)
      val positions = records.map(_._1.metadata.globalPosition)
      expect.all(
        records.size == expected.size,
        payloads == expected.map(_.payload),
        positions == expected.map(_.metadata.globalPosition),
      )
    }
  }

  test("fromBeginning = true reads records produced before the subscriber connects") { container =>
    val topic = uniqueId("from-beginning")
    val group = uniqueId("group")
    val expected = envelope(42L, "pre-existing")

    val program = KafkaModule.publisher[IO, TestEvent](producerConfig(container), codec).use { publisher =>
      for
        _        <- publisher.publish(topic, expected)
        received <- KafkaModule.subscriber[IO, TestEvent](consumerConfig(container, group), codec).use { subscriber =>
                      subscriber
                        .subscribe(topic, fromBeginning = true)
                        .take(1)
                        .compile
                        .toList
                        .timeout(30.seconds)
                    }
      yield received
    }

    program.map { records =>
      expect.all(
        records.size == 1,
        records.head._1.payload == expected.payload,
        records.head._1.metadata.globalPosition == expected.metadata.globalPosition,
      )
    }
  }

  test("multiple tags survive the JSON-array header round-trip") { container =>
    val topic = uniqueId("tags")
    val group = uniqueId("group")
    val tags = Set("user:1", "order:9", "session:42").map(Tag.fromString(_).get)
    val expected = envelope(1L, "multi-tag", tags = tags)

    val program = (
      KafkaModule.publisher[IO, TestEvent](producerConfig(container), codec),
      KafkaModule.subscriber[IO, TestEvent](consumerConfig(container, group), codec),
    ).tupled.use { case (publisher, subscriber) =>
      val consume = subscriber
        .subscribe(topic, fromBeginning = true)
        .take(1)
        .compile
        .toList
        .timeout(30.seconds)
      IO.both(consume, IO.sleep(2.seconds) *> publisher.publish(topic, expected)).map(_._1)
    }

    program.map(records => expect(records.head._1.metadata.tags == tags))
  }
