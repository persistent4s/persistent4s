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

/** Invalid pagination arguments rejected before querying PostgreSQL.
  *
  * @param message
  *   explanation of the invalid argument
  */
enum PaginationError(message: String) extends IllegalArgumentException(message):

  /** The requested row count is negative.
    *
    * @param value
    *   supplied row count
    */
  case NegativeLimit(value: Int) extends PaginationError(s"Query limit must not be negative: $value")

  /** The number of rows to skip is negative.
    *
    * @param value
    *   supplied offset
    */
  case NegativeOffset(value: Long) extends PaginationError(s"Query offset must not be negative: $value")
