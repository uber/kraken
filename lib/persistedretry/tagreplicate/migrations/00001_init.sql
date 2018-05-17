-- +goose Up
-- SQL in this section is executed when the migration is applied.
CREATE TABLE IF NOT EXISTS replicate_tag_task (
    tag          text      NOT NULL,
    digest       blob      NOT NULL,
    dependencies blob      NOT NULL,
    destination  text      NOT NULL,
    created_at   timestamp NOT NULL,
    last_attempt timestamp NOT NULL,
    status       text      NOT NULL,
    failures     integer   NOT NULL,
    PRIMARY KEY(tag, destination)
);

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.
DROP TABLE replicate_tag_task;
