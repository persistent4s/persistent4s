$version: "2"

namespace persistent4s.examples.courses.enrollment.api

use alloy#simpleRestJson

@simpleRestJson
service StudentService {
    operations: [
        RegisterStudent
        GetStudents
        GetStudent
    ]
}

@http(method: "POST", uri: "/students")
@idempotent
operation RegisterStudent {
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

    errors: [ValidationError]
}

@http(method: "GET", uri: "/students")
@readonly
operation GetStudents {
    output := {
        @required
        students: StudentList
    }
}

@http(method: "GET", uri: "/students/{studentId}")
@readonly
operation GetStudent {
    input := {
        @required
        @httpLabel
        studentId: String
    }

    output := {
        @required
        student: StudentItem
    }

    errors: [NotFoundError]
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
}
