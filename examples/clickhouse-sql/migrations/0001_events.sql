CREATE TABLE events (
  message String
) ENGINE = MergeTree ORDER BY message;
