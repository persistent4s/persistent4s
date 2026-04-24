CREATE TABLE IF NOT EXISTS books (
  book_id          UUID PRIMARY KEY,
  title            TEXT NOT NULL,
  author           TEXT NOT NULL,
  total_copies     INT  NOT NULL,
  available_copies INT  NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
  member_id     UUID PRIMARY KEY,
  name          TEXT NOT NULL,
  email         TEXT NOT NULL,
  borrowed_books INT  NOT NULL
);

CREATE TABLE IF NOT EXISTS borrowings (
  book_id     UUID        NOT NULL,
  member_id   UUID        NOT NULL,
  borrowed_at TIMESTAMPTZ NOT NULL,
  due_date    TIMESTAMPTZ NOT NULL,
  returned_at TIMESTAMPTZ,
  PRIMARY KEY (book_id, member_id)
);
