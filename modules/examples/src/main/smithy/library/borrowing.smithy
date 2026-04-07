$version: "2"

namespace persistent4s.examples.library.api

use alloy#simpleRestJson

@simpleRestJson
service BorrowingService {
    operations: [
        BorrowBook
        ReturnBook
        GetBorrowings
        GetActiveBorrowings
        GetMemberBorrowings
    ]
}

@http(method: "POST", uri: "/borrowings")
@idempotent
operation BorrowBook {
    input := {
        @required
        bookId: String

        @required
        memberId: String
    }
}

@http(method: "POST", uri: "/borrowings/{bookId}/{memberId}/return")
@idempotent
operation ReturnBook {
    input := {
        @httpLabel
        @required
        bookId: String

        @httpLabel
        @required
        memberId: String
    }
}

@http(method: "GET", uri: "/borrowings")
@readonly
operation GetBorrowings {
    output := {
        @required
        borrowings: BorrowingList
    }
}

@http(method: "GET", uri: "/borrowings/active")
@readonly
operation GetActiveBorrowings {
    output := {
        @required
        borrowings: BorrowingList
    }
}

@http(method: "GET", uri: "/members/{memberId}/borrowings")
@readonly
operation GetMemberBorrowings {
    input := {
        @required
        @httpLabel
        memberId: String
    }

    output := {
        @required
        borrowings: BorrowingList
    }
}

list BorrowingList {
    member: BorrowingItem
}

structure BorrowingItem {
    @required
    bookId: String

    @required
    memberId: String

    @required
    @timestampFormat("date-time")
    borrowedAt: Timestamp

    @required
    @timestampFormat("date-time")
    dueDate: Timestamp

    @timestampFormat("date-time")
    returnedAt: Timestamp
}
