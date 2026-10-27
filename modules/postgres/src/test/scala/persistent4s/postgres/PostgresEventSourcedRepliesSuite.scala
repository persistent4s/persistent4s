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

package persistent4s.postgres

import java.time.Instant

import cats.effect.{IO, Resource}
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import org.testcontainers.containers.PostgreSQLContainer
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

import persistent4s.circe.CirceEventCodec
import persistent4s.{
  CommandRuntime,
  Event,
  EventSchema,
  EventSourcedCommandHandler,
  IncomingMessage,
  MessageEncoder,
  PendingReply,
  RequestContext,
  SagaHeaders,
  Scope,
}

/** An event-sourced handler answering a saga request against the real store: the reply lands in the message outbox in
  * the transaction that appends the events, and a rejection is answered with no event at all.
  */
object PostgresEventSourcedRepliesSuite extends IOSuite:

  given org.typelevel.log4cats.Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class Fixture(components: PostgresModule.Components[IO, Granted], pool: Resource[IO, Session[IO]])

  type Res = Fixture

  final case class Grant(privilege: String)

  final case class Granted(privilege: String) extends Event derives Encoder, Decoder

  private val PrivilegeScope = Scope[String]("test.privilege")

  private given EventSchema[Granted] = EventSchema[Granted]("Granted").scopedBy(PrivilegeScope)(_.privilege)

  private given MessageEncoder[String] with

    def encode(message: String): Either[Throwable, String] = Right(message)

  /** Grants each privilege once; asking again is refused. */
  private object GrantHandler extends EventSourcedCommandHandler[Grant, Boolean, Granted, String]:

    protected val behavior = handler(false):
      scope(PrivilegeScope)(_.privilege)
      on[Granted].matching(_.privilege, _.privilege).evolve((_, _) => true)
      reject { case (true, command) => s"${command.privilege} is already granted" }
      emit(command => Granted(command.privilege))

  private val eventCodec = CirceEventCodec.make[Granted](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[Granted].left.map(error => error: Throwable),
  )

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))

  override def sharedResource: Resource[IO, Fixture] =
    postgresContainerResource.flatMap { container =>
      val config = PostgresConfig(
        host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
        password = container.getPassword, database = container.getDatabaseName, maxConnections = 16,
      )
      for
        components <- PostgresModule.makeWithConfig[IO, Granted](config, eventCodec, enableMessageOutbox = true)
        pool       <- Session
                  .Builder[IO]
                  .withHost(config.host)
                  .withPort(config.port)
                  .withUserAndPassword(config.user, config.password)
                  .withDatabase(config.database)
                  .pooled(4)
      yield Fixture(components, pool)
    }

  private def messageRows(pool: Resource[IO, Session[IO]]): IO[List[(String, Option[String], String)]] =
    pool
      .use(_.execute(sql"SELECT topic, message_key, payload FROM message_outbox ORDER BY id".query(text *: text.opt *: text)))
      .map(_.map { case topic *: key *: payload *: EmptyTuple => (topic, key, payload) })

  private def eventCount(pool: Resource[IO, Session[IO]]): IO[Long] =
    pool.use(_.unique(sql"SELECT count(*) FROM events".query(int8)))

  private val request = RequestContext(
    IncomingMessage(
      "employees.requests",
      Some("MENU_ACTIVITY_1"),
      "{}",
      Map(SagaHeaders.ReplyTo -> "activity.replies", SagaHeaders.Name -> "menu-saga", SagaHeaders.Id -> "instance-1"),
    ),
    Instant.EPOCH,
  )

  test("the reply is committed with the events, and a rejection is answered without one") { case Fixture(components, pool) =>
    given CommandRuntime[IO, Granted] = components.commandRuntime
    given PostgresEventStore[IO, Granted] = components.transactionalStore
    val answer = (_: Boolean, _: Grant, outcome: Either[String, List[Granted]]) =>
      Some(PendingReply(outcome.fold(reason => s"refused: $reason", _ => "granted")))
    for
      first  <- GrantHandler.runAnswering[IO](request, Grant("MENU_ACTIVITY_1"))(answer)
      second <- GrantHandler.runAnswering[IO](request, Grant("MENU_ACTIVITY_1"))(answer)
      events <- eventCount(pool)
      rows   <- messageRows(pool)
    yield expect(first.map(_.map(_.payload)) == Right(List(Granted("MENU_ACTIVITY_1")))) and
      expect(second == Left("MENU_ACTIVITY_1 is already granted")) and
      expect(events == 1L) and
      expect(
        rows == List(
          ("activity.replies", Some("instance-1"), "granted"),
          ("activity.replies", Some("instance-1"), "refused: MENU_ACTIVITY_1 is already granted"),
        ),
      )
  }
