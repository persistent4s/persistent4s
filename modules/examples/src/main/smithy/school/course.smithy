$version: "2"

namespace persistent4s.examples.school.api

use alloy#simpleRestJson

@simpleRestJson
service CourseService {
    operations: [
        CreateCourse
        GetCourses
    ]
}

@http(method: "POST", uri: "/courses")
@idempotent
operation CreateCourse {
    input := {
        @required
        title: String

        @required
        capacity: Integer
    }

    output := {
        @required
        courseId: String
    }
}

@http(method: "GET", uri: "/courses")
@readonly
operation GetCourses {
    output := {
        @required
        courses: CourseList
    }
}

list CourseList {
    member: CourseItem
}

structure CourseItem {
    @required
    courseId: String

    @required
    title: String

    @required
    capacity: Integer

    @required
    enrolledCount: Integer
}
