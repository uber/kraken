Kraken SQL Tag Datastore
========================
This backend is designed to speed up Docker tag operations for the build-index. It stores tags in a SQL database rather
than as files on the same backend storage as the blobs. While this adds an additional component to the Kraken,
deployment it significantly enhances the performance of the build-index service, especially with registries that have a
large number of Docker repositories or repositories with a large number of tags. This can eliminate the need for
things like an nginx cache or result pagination that attempt to compensate for slow tag performance.

For example, Uber ATG's deployment of Kraken in had (as of August 2019) about 30,000 Docker repositories, with some
repositories having over 16,000 tags.
1. Listing tags the 16,353 tags in one particular repository took between 35 and 50 seconds, and often resulted in a
`500 Server Errror` returned to the user. 
2. Listing 29,660 repositories in the Docker catalog took over 3 minutes and also often results in an error.

Using the SQL Tag Datastore:
1. Listing 100,000 tags in a single repository takes 0.5 seconds.
2. Listing 100,000 repositories in the Docker catalog takes 0.9 seconds.

Supported Databases
-------------------
The backend uses [GORM](http://gorm.io/) as the database interface, so it can support any database supported by GORM. 
By default it is only configured to use MySQL and SQLite (the latter only for testing), so see the GORM documentation
on how to configure it to use other databases such as Postgres.

Benchmarks
----------
Under the `benchmark` package there are a set of Golang benchmark tests that can be used to evaluate the performance of
the SQL Tag Datastore. These are kept separate from the standard set of unit tests because they create a much larger
test dataset, which can slow down the Bazel tests. Here is an example of the benchmarks output:
```
pkg: github.com/uber/kraken/lib/backend/sqlbackend/benchmark
BenchmarkStat-8          	   20000	     64071 ns/op
BenchmarkDownload-8      	   30000	     61641 ns/op
2019/09/03 13:57:25 Docker catalog took 617.229428ms for 100001 repos
2019/09/03 13:57:26 Docker catalog took 531.105741ms for 100001 repos
2019/09/03 13:57:26 Docker catalog took 534.448273ms for 100001 repos
BenchmarkListCatalog-8   	       2	 532797142 ns/op
2019/09/03 13:57:27 Docker tag list took 460.731339ms for 100000 tags
2019/09/03 13:57:27 Docker tag list took 450.263591ms for 100000 tags
2019/09/03 13:57:28 Docker tag list took 447.621031ms for 100000 tags
2019/09/03 13:57:28 Docker tag list took 460.523944ms for 100000 tags
2019/09/03 13:57:29 Docker tag list took 447.264241ms for 100000 tags
2019/09/03 13:57:29 Docker tag list took 454.095311ms for 100000 tags
BenchmarkListTags-8      	       3	 453979596 ns/op
PASS
```
