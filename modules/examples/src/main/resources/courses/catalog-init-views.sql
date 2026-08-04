CREATE TABLE IF NOT EXISTS courses (
  course_id  UUID PRIMARY KEY,
  code       TEXT NOT NULL,
  title      TEXT NOT NULL,
  capacity   INT  NOT NULL,
  instructor TEXT NOT NULL,
  is_open    BOOLEAN NOT NULL
);
