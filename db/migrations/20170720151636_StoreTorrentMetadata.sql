
-- +goose Up
-- SQL in section 'Up' is executed when this migration is applied
ALTER TABLE torrent 
    DROP COLUMN numPieces,
    DROP COLUMN pieceLength,
    ADD COLUMN metaInfo text,
    CHANGE COLUMN torrentName name varchar(255),
    CHANGE COLUMN refcount refCount int;

ALTER TABLE manifest
    CHANGE COLUMN tagName tag varchar(255),
    CHANGE COLUMN manifest data text;

ALTER TABLE peer
    DROP COLUMN bytes_uploaded,
    DROP COLUMN bytes_left,
    DROP COLUMN event,
    ADD COLUMN dc char(10);

-- +goose Down
-- SQL section 'Down' is executed when this migration is rolled back

ALTER TABLE peer
    DROP COLUMN dc,
    ADD COLUMN event char(255),
    ADD COLUMN bytes_left bigint,
    ADD COLUMN bytes_uploaded bigint;
    
ALTER TABLE manifest
    CHANGE COLUMN data manifest text,
    CHANGE COLUMN tag tagName varchar(255);

ALTER TABLE torrent
    CHANGE COLUMN refCount refcount int,
    CHANGE COLUMN name torrentName varchar(255),
    DROP COLUMN metadata,
    ADD COLUMN pieceLength int,
    ADD COLUMN numPieces int;
