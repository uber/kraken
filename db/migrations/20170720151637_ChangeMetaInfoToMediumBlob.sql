-- +goose Up
-- SQL in this section is executed when the migration is applied.

ALTER TABLE torrent MODIFY metaInfo MEDIUMBLOB;

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.

ALTER TABLE torrent MODIFY metaInfo TEXT;
