CREATE TABLE IF NOT EXISTS students (
  student_id UUID PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS enrollments (
  student_id  UUID NOT NULL,
  course_id   UUID NOT NULL,
  enrolled_at TIMESTAMPTZ NOT NULL,
  dropped_at  TIMESTAMPTZ,
  PRIMARY KEY (student_id, course_id)
);

-- Fed by the Kafka subscriber, not by a local projection.
CREATE TABLE IF NOT EXISTS course_view (
  course_id  UUID PRIMARY KEY,
  code       TEXT NOT NULL,
  title      TEXT NOT NULL,
  capacity   INT  NOT NULL,
  instructor TEXT NOT NULL,
  is_open    BOOLEAN NOT NULL
);
