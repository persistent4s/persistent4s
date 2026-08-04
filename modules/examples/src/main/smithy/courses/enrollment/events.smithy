$version: "2"

namespace persistent4s.examples.courses.enrollment.api

use alloy#simpleRestJson

@simpleRestJson
service EventsService {
    operations: [
        GetEvents
    ]
}

@http(method: "GET", uri: "/events")
@readonly
operation GetEvents {
    output := {
        @required
        events: EventList
    }
}

list EventList {
    member: EventItem
}

structure EventItem {
    @required
    globalPosition: Long

    @required
    eventType: String

    @required
    tags: TagList

    @required
    @timestampFormat("date-time")
    timestamp: Timestamp
}

list TagList {
    member: String
}
