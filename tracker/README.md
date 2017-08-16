# Database schema
Please rememeber to update schema if you want to do a db migration.

## Manifest
+----------+--------------+------+-----+---------+-------+
| Field    | Type         | Null | Key | Default | Extra |
+----------+--------------+------+-----+---------+-------+
| tag      | varchar(255) | NO   | PRI |         |       |
| data     | text         | YES  |     | NULL    |       |
| flags    | int(11)      | YES  |     | NULL    |       |
+----------+--------------+------+-----+---------+-------+

## Torrent
+-------------+--------------+------+-----+---------+-------+
| Field       | Type         | Null | Key | Default | Extra |
+-------------+--------------+------+-----+---------+-------+
| name        | varchar(255) | NO   | PRI |         |       |
| infoHash    | char(64)     | YES  |     | NULL    |       |
| author      | char(255)    | YES  |     | NULL    |       |
| flags       | int(11)      | YES  |     | NULL    |       |
| refCount    | int(11)      | NO   |     | 0       |       |
| metaInfo    | text         | YES  |     | NULL    |       |
+-------------+--------------+------+-----+---------+-------+

## Peer
+------------------+------------+------+-----+---------+-------+
| Field            | Type       | Null | Key | Default | Extra |
+------------------+------------+------+-----+---------+-------+
| infoHash         | char(64)   | NO   | PRI |         |       |
| peerId           | char(255)  | NO   | PRI |         |       |
| dc               | char(10)   | YES  |     | NULL    |       |
| ip               | char(40)   | YES  |     | NULL    |       |
| port             | bigint(20) | YES  |     | NULL    |       |
| bytes_downloaded | bigint(20) | YES  |     | NULL    |       |
| flags            | int(11)    | YES  |     | NULL    |       |
+------------------+------------+------+-----+---------+-------+
