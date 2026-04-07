$version: "2"

namespace persistent4s.examples.library.api

use alloy#simpleRestJson

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
}

@http(method: "GET", uri: "/books")
@readonly
operation GetBooks {
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
}

list BookList {
    member: BookItem
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
