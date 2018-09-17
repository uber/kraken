0.0.14 (2018-09-17)
====
- Acceptance test improvements (@codyg)
- Make kraken-agent in atg fallback to artifactory (@evelynl)

0.0.13 (2018-09-12)
====
- Allow specifying multiple custom ports for proxy (@codyg)
- Run IRN proxy nginx as root to listen on port 80 (@codyg)

0.0.12 (2018-09-11)
====
- Do not error when file is in download state before scheduler download (@codyg)
- Set torrentlog connection stats to debug level (@codyg)

0.0.11 (2018-09-07)
====
- Add /forcecleanup endpoint to origin for manually freeing space (@codyg)
- Fix torrent completeness race (@codyg)

0.0.10 (2018-09-05)
====
- Fix WBU2 config (@codyg)
- Fix torrentlog timestamp (@codyg)
- Do not swallow file deletion errors (@yiran)
- Fix nginx header for opus client (@yiran)
- Always attempt to write-back when returning 409 in origin blob upload (@codyg)

0.0.9 (2018-08-28)
====
- Fix sjc4/dca6 config (@codyg)
- Add phx3 config (@yiran)
- Remove unnecessary origin torrent archive log (@codyg)
- Add retry to httpbackend / clean up code (@codyg)
- Updated terrablob client (@magnus)
- Use seconds instead of nanos for log durations (@codyg)
- Fix scheduler log in constructor (@evelynl)
- Disable stacktrace (@evelynl)
- Log download time on blob refresh (@codyg)
- Remove log on announcing disabled and file persisted (@evelynl)
- Add scheduler.log in preprod (@codyg)
- Enable DNS in preprod cluster (@codyg)
- Let proxy always return its own hostname in location header (@yiran)
- Remove retries from build-index / tracker clients (@codyg)
- Run passive health checks from agent->tracker (@codyg)
- Enable passive health checks from agent->build-index (@codyg)
- Add active health check from proxy->build-index (@codyg)
- Implement passive health checks (@codyg)
- Run health checks between build-index (@codyg)
- Emit metrics for agent tag lookup failures (@codyg)
- Do not restrict origin access on incorrect location (@codyg)
- Run active health checks on origin cluster from build-index / tracker / proxy (@codyg)
- Move scheduler to its own logger (@evelynl)
- Move port to hostlist config (@codyg)
- Clean up integration test component addresses (@codyg)
- Make hostlist.List resilient to errors (@codyg)
- Parameterize nginx configuration templates (@codyg)
- Fix refresh bug (@codyg)
- Generate random images for upload for acceptance tests (@codyg)
- Check health of hash nodes in hashring (@codyg)
- Remove Monitor from healthcheck (@codyg)
- Apply defaults on config in hashring (@codyg)
- Encapsulate origin hashing logic in hashring package (@codyg)
- DCA6 cluster config (@yiran)
- Enhance proxy acceptance tests (@codyg)
- Move generated nginx confs from /etc to /tmp (@codyg)
- Monitor health of list of hosts (@codyg)
- Refactor RendezvousHash tests (@yiran)
- Add replication to sjc4 and dca4 (@evelynl)
- Make hostlist.List an interface for mocking purposes (@codyg)
- Make stripping local host from hostlist optional (@codyg)
- Add script for enabling Kraken for prime services (@codyg)

0.0.8 (2018-08-06)
====
- Cache origin peer contexts / errors in tracker (@codyg)
- Lower timeouts in blobclient (@codyg)

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
