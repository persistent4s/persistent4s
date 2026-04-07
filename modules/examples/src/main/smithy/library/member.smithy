$version: "2"

namespace persistent4s.examples.library.api

use alloy#simpleRestJson

@simpleRestJson
service MemberService {
    operations: [
        RegisterMember
        GetMembers
        GetMember
    ]
}

@http(method: "POST", uri: "/members")
@idempotent
operation RegisterMember {
    input := {
        @required
        name: String

        @required
        email: String
    }

    output := {
        @required
        memberId: String
    }
}

@http(method: "GET", uri: "/members")
@readonly
operation GetMembers {
    output := {
        @required
        members: MemberList
    }
}

@http(method: "GET", uri: "/members/{memberId}")
@readonly
operation GetMember {
    input := {
        @required
        @httpLabel
        memberId: String
    }

    output := {
        @required
        member: MemberItem
    }
}

list MemberList {
    member: MemberItem
}

structure MemberItem {
    @required
    memberId: String

    @required
    name: String

    @required
    email: String

    @required
    borrowedBooks: Integer
}
