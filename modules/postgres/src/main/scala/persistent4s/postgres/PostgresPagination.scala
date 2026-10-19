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

private[postgres] object PostgresPagination:

  def cap(count: Int, maxLimit: Int): Int = count.min(maxLimit)

  def validate(limit: Option[Int], offset: Option[Long], maxLimit: Int): Unit =
    require(maxLimit >= 0, "PostgreSQL query maximum limit must not be negative")
    limit.foreach { count =>
      if count < 0 then throw PaginationError.NegativeLimit(count)
    }
    offset.foreach { count =>
      if count < 0 then throw PaginationError.NegativeOffset(count)
    }
