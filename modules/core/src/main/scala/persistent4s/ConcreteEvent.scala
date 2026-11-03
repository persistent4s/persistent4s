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

package persistent4s

import scala.quoted.*

/** Evidence that `E` is not a sealed family of events. A sealed trait or enum is never stored under its own name, so a
  * handler declared with `on` for it, in an [[EventSourcedProjection]] or an [[EventSourcedCommandHandler]], would
  * never run.
  */
sealed trait ConcreteEvent[E]

object ConcreteEvent:

  private[persistent4s] object Instance extends ConcreteEvent[Any]

  /** Available for every type except a sealed trait, sealed abstract class, or enum. Abstract type parameters and
    * singleton enum cases are accepted.
    */
  inline given [E]: ConcreteEvent[E] = ${ ConcreteEventMacros.evidence[E] }

private object ConcreteEventMacros:

  def evidence[E: Type](using Quotes): Expr[ConcreteEvent[E]] =
    import quotes.reflect.*

    val eventType = TypeRepr.of[E].dealias
    val symbol = eventType.typeSymbol
    val isFamily =
      !eventType.isSingleton && symbol.isClassDef && symbol.flags.is(Flags.Sealed) &&
        (symbol.flags.is(Flags.Trait) || symbol.flags.is(Flags.Abstract))

    if isFamily then
      report.errorAndAbort(
        s"${symbol.name} is a sealed family of events, which is never stored under its own name. " +
          s"Declare on with each of its concrete events, or use onFamily[${symbol.name}] in an EventSourcedProjection.",
      )

    '{ ConcreteEvent.Instance.asInstanceOf[ConcreteEvent[E]] }
