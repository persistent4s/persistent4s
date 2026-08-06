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

import java.nio.charset.StandardCharsets

private[postgres] object SqlIdentifier:

  private val ValidPart = "[A-Za-z_][A-Za-z0-9_$]*".r

  private val MaxBytes = 63

  def table(value: String): String =
    val parts = value.split("\\.", -1).toList
    require(parts.nonEmpty && parts.forall(isValid), s"Invalid PostgreSQL table name [$value]")
    parts.map(quote).mkString(".")

  def column(value: String): String =
    require(isValid(value), s"Invalid PostgreSQL column name [$value]")
    quote(value)

  private def isValid(value: String): Boolean =
    ValidPart.matches(value) && value.getBytes(StandardCharsets.UTF_8).length <= MaxBytes

  private def quote(value: String): String =
    s"\"$value\""
