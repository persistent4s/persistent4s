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

/** A [[CommandHandler]] whose command arrived as a saga request, and which answers it.
  *
  * The reply is enqueued in the transaction that appends the events, which is exactly why [[SagaParticipant]] cannot
  * send it afterwards: "the stock is reserved" and "I told them it is reserved" have to become true together, or a
  * crash between the two strands the asking saga until its deadline.
  *
  * Opt-in, rather than something every [[CommandHandler]] carries. A handler that answers nobody — a compensation, a
  * plain local command — should not have to know that sagas exist.
  */
trait SagaCommandHandler[C, S, E <: Event] extends CommandHandler[C, S, E]:

  /** The request being answered: the address to reply to, and the clock reading that decides whether it is stale.
    *
    * Its presence is why an implementation is a '''case class''' rather than an object — one instance per request,
    * which is free, and the alternative would be threading the message through every decision function that might need
    * it.
    */
  def request: RequestContext

  /** What to answer, or `None` to answer nothing.
    *
    * `outcome` is `Left` when [[validate]] rejected the command. Answering precisely when it writes nothing is the
    * whole job of a partner that can say no: a rejection the caller never hears about is indistinguishable from a
    * partner that has died, and costs the asking saga its full deadline to discover.
    */
  def reply(state: S, command: C, outcome: Either[Throwable, List[E]]): Option[PendingReply]

  /** Encodes [[reply]] and addresses it from [[request]], so a handler never touches [[SagaHeaders]] itself.
    *
    * Yields nothing when the request nominated nowhere to answer — a command from something that is not a saga, which
    * is legitimate. That is silent here on purpose: this is pure code with no logger, and [[SagaParticipant]] has
    * already warned about the unaddressed request before dispatching it.
    */
  final override def messages(
    state: S,
    command: C,
    outcome: Either[Throwable, List[E]],
  ): Either[Throwable, List[OutgoingMessage]] =
    reply(state, command, outcome) match
      case None          => Right(Nil)
      case Some(pending) =>
        pending.encoded.map(payload => SagaHeaders.reply(request.message, payload, headers = pending.headers).toList)
