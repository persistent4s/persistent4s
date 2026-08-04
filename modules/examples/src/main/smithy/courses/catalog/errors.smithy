$version: "2"

namespace persistent4s.examples.courses.catalog.api

@error("client")
@httpError(404)
structure NotFoundError {
    @required
    message: String
}

@error("client")
@httpError(400)
structure ValidationError {
    @required
    message: String
}
