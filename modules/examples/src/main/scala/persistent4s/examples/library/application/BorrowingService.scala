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

package persistent4s.examples.library.application

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.borrowing.*
import persistent4s.examples.library.infrastructure.LibraryModule
import smithy4s.Timestamp

class BorrowingServiceImpl(module: LibraryModule) extends BorrowingService[IO]:

  private given EventStore[IO, LibraryEvent] = module.store

  def borrowBook(bookId: String, memberId: String): IO[Unit] =
    BorrowBookHandler.run[IO](BorrowBook(bookId, memberId))

  def returnBook(bookId: String, memberId: String): IO[Unit] =
    ReturnBookHandler.run[IO](ReturnBook(bookId, memberId))

  def getBorrowings(): IO[GetBorrowingsOutput] =
    module.borrowingProjection.getBorrowings.map(borrowings => GetBorrowingsOutput(borrowings.map(toBorrowingItem)))

  def getActiveBorrowings(): IO[GetActiveBorrowingsOutput] =
    module.borrowingProjection.getActiveBorrowings.map(borrowings =>
      GetActiveBorrowingsOutput(borrowings.map(toBorrowingItem)),
    )

  def getMemberBorrowings(memberId: String): IO[GetMemberBorrowingsOutput] =
    module.borrowingProjection
      .getBorrowingsByMember(memberId)
      .map(borrowings => GetMemberBorrowingsOutput(borrowings.map(toBorrowingItem)))

  private def toBorrowingItem(b: BorrowingView): BorrowingItem =
    BorrowingItem(
      bookId = b.bookId,
      memberId = b.memberId,
      borrowedAt = Timestamp.fromInstant(b.borrowedAt.toInstant),
      dueDate = Timestamp.fromInstant(b.dueDate.toInstant),
      returnedAt = b.returnedAt.map(returnedAt => Timestamp.fromInstant(returnedAt.toInstant)),
    )
