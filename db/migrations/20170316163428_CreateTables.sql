
-- +goose Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS Peer (
    infoHash char(64),
    peerId char(255),
    ip char(40),
    port char(10),
    bytes_downloaded bigint,
    bytes_uploaded bigint,
    bytes_left bigint,
    event char(255),
    flags int,
    PRIMARY KEY(infoHash, peerId)
);

CREATE TABLE IF NOT EXISTS Torrent (
    torrentName varchar(2047),
    author char(255),
    numPieces int,
    pieceLength int,
    flags int,
    PRIMARY KEY(torrentName)
);

-- +goose Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP TABLE Peer;
DROP TABLE Torrent;
