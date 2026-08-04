$version: "2"

namespace persistent4s.examples.courses.enrollment.api

use alloy#simpleRestJson

@simpleRestJson
service CourseViewService {
    operations: [
        GetCourseView
        GetCourseViewItem
    ]
}

@http(method: "GET", uri: "/course-view")
@readonly
operation GetCourseView {
    output := {
        @required
        courses: CourseViewList
    }
}

@http(method: "GET", uri: "/course-view/{courseId}")
@readonly
operation GetCourseViewItem {
    input := {
        @required
        @httpLabel
        courseId: String
    }

    output := {
        @required
        course: CourseViewItem
    }

    errors: [NotFoundError]
}

list CourseViewList {
    member: CourseViewItem
}

structure CourseViewItem {
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
