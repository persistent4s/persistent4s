$version: "2"

namespace persistent4s.examples.courses.catalog.api

use alloy#simpleRestJson

@simpleRestJson
service CourseService {
    operations: [
        OpenCourse
        ChangeCapacity
        CloseCourse
        GetCourses
        GetCourse
    ]
}

@http(method: "POST", uri: "/courses")
@idempotent
operation OpenCourse {
    input := {
        @required
        code: String

        @required
        title: String

        @required
        capacity: Integer

        @required
        instructor: String
    }

    output := {
        @required
        courseId: String
    }

    errors: [ValidationError]
}

@http(method: "POST", uri: "/courses/{courseId}/capacity")
@idempotent
operation ChangeCapacity {
    input := {
        @httpLabel
        @required
        courseId: String

        @required
        newCapacity: Integer
    }

    errors: [ValidationError, NotFoundError]
}

@http(method: "POST", uri: "/courses/{courseId}/close")
@idempotent
operation CloseCourse {
    input := {
        @httpLabel
        @required
        courseId: String
    }

    errors: [ValidationError, NotFoundError]
}

@http(method: "GET", uri: "/courses")
@readonly
operation GetCourses {
    output := {
        @required
        courses: CourseList
    }
}

@http(method: "GET", uri: "/courses/{courseId}")
@readonly
operation GetCourse {
    input := {
        @required
        @httpLabel
        courseId: String
    }

    output := {
        @required
        course: CourseItem
    }

    errors: [NotFoundError]
}

list CourseList {
    member: CourseItem
}

structure CourseItem {
    @required
    courseId: String

    @required
    code: String

    @required
    title: String

    @required
    capacity: Integer

    @required
    instructor: String

    @required
    isOpen: Boolean
}
