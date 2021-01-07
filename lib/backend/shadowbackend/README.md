Kraken Shadow Datastore
========================
This backend is designed for transitioning Kraken to a new backend by allowing admins to specify an "active" and
"shadow" backend. The term shadow is used because the other backend "shadows" the active backend. Writes are sent 
to both backends, but reads only occur from the active. This ensures data consistency between the backends, and 
allows the old, proven backed to act as a safety net in the case where the new backend fails or has to be taken
offline due to some unforeseen problems.

Because all reads occur from the active backend, data needs to be manually migrated from the old backend to the new,
before bringing the shadow backend online. This means there will be some downtime incurred during the transition.

Supported Backends
------------------
This currently supports SQL, HDFS, S3, and testfs backends. 

Note that this backend is only enabled as a tag datastore for the build-index. However this could be used as the blob
datastore for the origin servers, provided it is not configured to use the SQL backend in active or shadow modes. 

Configuration
-------------
The config has two required items "active_backend" and "shadow_backend", each of which function the same as the
standard "backend" in that the backend type and normal configuration is defined under each, depending on which 
role that backend will play. For example, here is a configuration that uses the SQL backend as the active, and
the testfs backend as the shadow:
```yaml
backends:
  - namespace: .*
    backend:
      shadow:
        active_backend:
          sql:
            dialect: mysql
            connection_string: "kraken:kraken@tcp(kraken-mysql:3306)/kraken?parseTime=True"
            debug_logging: true
        shadow_backend:
          testfs:
            addr: localhost:7357
            root: tags
            name_path: docker_tag
``` 
