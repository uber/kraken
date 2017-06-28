
-- +goose Up
-- SQL in section 'Up' is executed when this migration is applied

ALTER TABLE torrent MODIFY refcount int NOT NULL DEFAULT 0;

-- +goose Down
-- SQL section 'Down' is executed when this migration is rolled back

ALTER TABLE torrent MODIFY refcount int NOT NULL DEFAULT 1;
