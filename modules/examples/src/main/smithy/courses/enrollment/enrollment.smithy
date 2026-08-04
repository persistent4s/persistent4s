$version: "2"

namespace persistent4s.examples.courses.enrollment.api

use alloy#simpleRestJson

@simpleRestJson
service EnrollmentService {
    operations: [
        EnrollStudent
        DropStudent
        GetEnrollments
        GetStudentEnrollments
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

    errors: [ValidationError, NotFoundError]
}

@http(method: "POST", uri: "/enrollments/{studentId}/{courseId}/drop")
@idempotent
operation DropStudent {
    input := {
        @httpLabel
        @required
        studentId: String

        @httpLabel
        @required
        courseId: String
    }

    errors: [ValidationError, NotFoundError]
}

@http(method: "GET", uri: "/enrollments")
@readonly
operation GetEnrollments {
    output := {
        @required
        enrollments: EnrollmentList
    }
}

@http(method: "GET", uri: "/students/{studentId}/enrollments")
@readonly
operation GetStudentEnrollments {
    input := {
        @required
        @httpLabel
        studentId: String
    }

    output := {
        @required
        enrollments: EnrollmentList
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
        enrollments: EnrollmentList
    }
}

list EnrollmentList {
    member: EnrollmentItem
}

structure EnrollmentItem {
    @required
    studentId: String

    @required
    courseId: String

    @required
    @timestampFormat("date-time")
    enrolledAt: Timestamp

    @timestampFormat("date-time")
    droppedAt: Timestamp
}
