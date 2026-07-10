$version: "2"

namespace persistent4s.examples.library.api

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

@error("server")
@httpError(503)
structure ProjectionTimeoutError {
    @required
    message: String
}
