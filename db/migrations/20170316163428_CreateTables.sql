-- +goose Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS peer (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS torrent (
    torrentName varchar(255),
    infoHash char(64),
    author char(255),
    numPieces int,
    pieceLength int,
    flags int,
    PRIMARY KEY(torrentName)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- +goose Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP TABLE Peer;
DROP TABLE Torrent;
