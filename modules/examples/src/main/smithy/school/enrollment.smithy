$version: "2"

namespace persistent4s.examples.school.api

use alloy#simpleRestJson

@simpleRestJson
service EnrollmentService {
    operations: [
        EnrollStudent
        GetCourseEnrollments
    ]
}

@http(method: "POST", uri: "/enrollments")
@idempotent
operation EnrollStudent {
    input := {
        @required
        studentId: String

        @required
        courseId: String
    }
}

@http(method: "GET", uri: "/courses/{courseId}/enrollments")
@readonly
operation GetCourseEnrollments {
    input := {
        @required
        @httpLabel
        courseId: String
    }

    output := {
        @required
        studentIds: StudentIdList
    }
}

list StudentIdList {
    member: String
}
