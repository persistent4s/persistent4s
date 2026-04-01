$version: "2"

namespace persistent4s.examples.school.api

use alloy#simpleRestJson

@simpleRestJson
service StudentService {
    operations: [
        CreateStudent
        DeleteStudent
        GetStudents
    ]
}

@http(method: "POST", uri: "/students")
@idempotent
operation CreateStudent {
    input := {
        @required
        name: String

        @required
        email: String
    }

    output := {
        @required
        studentId: String
    }
}

@http(method: "DELETE", uri: "/students/{studentId}")
@idempotent
operation DeleteStudent {
    input := {
        @required
        @httpLabel
        studentId: String
    }
}

@http(method: "GET", uri: "/students")
@readonly
operation GetStudents {
    output := {
        @required
        students: StudentList
    }
}

list StudentList {
    member: StudentItem
}

structure StudentItem {
    @required
    studentId: String

    @required
    name: String

    @required
    email: String

    @required
    enrollmentCount: Integer
}
