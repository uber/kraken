-- +goose Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS manifest (
    tagName varchar(255),
    manifest text,
    flags int,
    PRIMARY KEY(tagName)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE torrent ADD COLUMN (refcount int NOT NULL DEFAULT 1);

-- +goose Down
-- SQL section 'Down' is executed when this migration is rolled back

DROP TABLE IF EXISTS manifest;

ALTER TABLE torrent DROP COLUMN `refcount`;

