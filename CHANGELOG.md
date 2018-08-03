0.0.7 (2018-08-03)
====
- SJC4/DCA4 configuration files (@codyg/@yiran)
- Lower list concurrency / cache tag lists to prevent HDFS errors (@codyg)
- Include original HDFS error when all name nodes fail (@codyg)
- Make limiter task runner determine ttl (@codyg)

0.0.6 (2018-08-01)
====
- Enable agent registry fallback in IRN (@codyg)
- Add configuration for IRN preprod (@codyg)
- Handle intermediate directories in release tooling (@eoakes)
- Remove hostname from origin stats (@codyg)
- Change tracker v2 announce to POST (@codyg)
- Move nginx into agent process (@codyg)
- Reduce image size (@yiran)
- Fix origin pinocchio (@yiran)
- Enhance persistedretry logging / monitoring (@codyg)
- Fix list for file entry name contains slashes (@evelynl)
- Enable replication in prime (@evelynl)
- Migrate proxy to port 5000 (@yiran)

0.0.5 (2018-07-26)
====
- Add writeback flag to build-index (@evelynl)
- Add new wbu2 kraken_herd hosts to hash node config (@codyg)
- Fix IRN redis addr (@codyg)
- Add acceptance testing tools (@codyg)
- Add off-by-default mutual connection threshold to scheduler (@eoakes)

0.0.4 (2018-07-24)
====
- Make tag list backwards compatible (@codyg)
- Replace SJC1/DCA1 origin hosts (@yiran)
- Add release tooling (@codyg)

0.0.3 (2018-07-23)
====
- Fix build-index preprod M3 port (@codyg)
- Switch /var/run usage to /tmp (@codyg)
- Remove shuffling for upload origin location and add StatusConflict handling in origin uploader (@evelynl)

0.0.2 (2018-07-23)
====
- Fix version emitting (@codyg)

0.0.1 (2018-07-23)
====
- Initial release (@codyg)

0.0.0 (2018-06-28)
====
- Alpha release for test image (@codyg)
