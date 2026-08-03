$version: "2"

namespace persistent4s.examples.library.api

use alloy#simpleRestJson
use smithy4s.meta#packedInputs

@simpleRestJson
service BookService {
    operations: [
        AddBook
        GetBooks
        GetBook
    ]
}

@http(method: "POST", uri: "/books")
@idempotent
operation AddBook {
    input := {
        @required
        title: String

        @required
        author: String

        @required
        totalCopies: Integer
    }

    output := {
        @required
        bookId: String
    }

    errors: [ValidationError]
}

@http(method: "GET", uri: "/books")
@readonly
@packedInputs
operation GetBooks {
    input := {
        @httpQuery("title")
        title: StringList

        @httpQuery("author")
        author: StringList

        @httpQuery("totalCopies")
        totalCopies: Integer

        @httpQuery("availableCopies")
        availableCopies: Integer
    }

    output := {
        @required
        books: BookList
    }
}

@http(method: "GET", uri: "/books/{bookId}")
@readonly
operation GetBook {
    input := {
        @required
        @httpLabel
        bookId: String
    }

    output := {
        @required
        book: BookItem
    }

    errors: [NotFoundError]
}

list BookList {
    member: BookItem
}

list StringList {
    member: String
}

structure BookItem {
    @required
    bookId: String

    @required
    title: String

    @required
    author: String

    @required
    totalCopies: Integer

    @required
    availableCopies: Integer
}
