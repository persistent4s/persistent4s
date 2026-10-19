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

import cats.effect.{IO, Ref}
import cats.syntax.all.*

import weaver.SimpleIOSuite

object PostgresQuerySuite extends SimpleIOSuite:

  private def query = PostgresQuery.paginated[IO, (Int, Long)]((limit, offset) => IO.pure((limit, offset)))

  test("caps custom queries using the same defaults and configurable maximum as derived queries"):
    val cases = List(
      query                                         -> (200, 0L),
      query.withLimit(50)                           -> (50, 0L),
      query.withLimit(5000).withOffset(20)          -> (200, 20L),
      query.withLimit(Int.MaxValue, maxLimit = 300) -> (300, 0L),
      query.withLimit(50, maxLimit = 300)           -> (50, 0L),
      query.withLimit(5000, maxLimit = 0)           -> (0, 0L),
      query.withLimit(0).withOffset(Long.MaxValue)  -> (0, Long.MaxValue),
    )
    cases.traverse((page, expected) => page.run.map(actual => expect.eql(expected, actual))).map(_.reduce(_ and _))

  test("rejects invalid values before invoking the custom query, including for zero limits"):
    for
      calls    <- Ref.of[IO, Int](0)
      base      = PostgresQuery.paginated[IO, Unit]((_, _) => calls.update(_ + 1))
      failures <- List(
                    base.withLimit(-1)                          -> PaginationError.NegativeLimit(-1),
                    base.withLimit(Int.MinValue, maxLimit = 0)  -> PaginationError.NegativeLimit(Int.MinValue),
                    base.withOffset(-1)                         -> PaginationError.NegativeOffset(-1),
                    base.withLimit(0).withOffset(Long.MinValue) -> PaginationError.NegativeOffset(Long.MinValue),
                  ).traverse { (page, expected) =>
                    (page.validate.attempt, page.run.attempt).mapN { (validation, execution) =>
                      expect(validation.left.exists(_ == expected)) and expect(execution.left.exists(_ == expected))
                    }
                  }
      count <- calls.get
    yield failures.reduce(_ and _) and expect(count == 0)

  test("rejects a negative maximum before invoking the query"):
    for
      calls   <- Ref.of[IO, Int](0)
      failure <-
        PostgresQuery.paginated[IO, Unit]((_, _) => calls.update(_ + 1)).withLimit(0, maxLimit = -1).run.attempt
      count <- calls.get
    yield expect(
      failure.left.exists(error => error.isInstanceOf[IllegalArgumentException] && !error.isInstanceOf[PaginationError]),
    ) and expect(count == 0)

  test("keeps builders immutable and allows replacing an invalid limit or offset"):
    val base = query.withLimit(5000, maxLimit = 300).withOffset(20)
    for
      original  <- base.run
      changed   <- base.withLimit(5000).withOffset(30).run
      corrected <- base.withLimit(-1).withOffset(-1).withLimit(0).withOffset(0).run
    yield expect.eql((300, 20L), original) and expect.eql((200, 30L), changed) and expect.eql((0, 0L), corrected)

  test("validation does not run the query and a zero limit still runs it to return totals"):
    for
      calls <- Ref.of[IO, Int](0)
      page   = PostgresQuery
               .paginated[IO, (Int, List[Int])]((limit, _) => calls.update(_ + 1).as((203, List.fill(limit)(1))))
               .withLimit(0)
      _      <- page.validate
      before <- calls.get
      result <- page.run
      after  <- calls.get
    yield expect(before == 0) and expect(after == 1) and expect.eql((203, List.empty[Int]), result)

  test("defers callback construction and preserves its failures"):
    val failure = new Exception("Synthetic query failure")
    val page = PostgresQuery.paginated[IO, Unit]((_, _) => throw failure)
    page.run.attempt.map(result => expect(result.left.exists(_ eq failure)))
