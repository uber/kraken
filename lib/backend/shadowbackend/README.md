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
The active and shadow backends can use any factory registered with `backend.Register`.
SQL, HDFS, S3, and testfs are registered automatically by this package. Other built-in
backends and custom implementations must be imported or registered by the application.

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

Custom Backends
---------------
To use a custom backend in either role:

1. Implement [`backend.Client`](../client.go) and [`backend.ClientFactory`](../client.go).
   The factory's `Create` method parses its configuration and authentication settings
   and returns the client.
2. Register the factory under a unique name before creating the shadow client. For
   example, a custom backend package can register its factory in `init`:

   ```go
   func init() {
       backend.Register("custom", &factory{})
   }
   ```

3. Import the custom package into the application that constructs the shadow backend.
   If it is only needed for registration, use a blank import:

   ```go
   import _ "example.com/myapp/custombackend"
   ```

   Registration happens in the running binary; adding a name to YAML does not load a
   backend implementation.
4. Use the registered name as the key under `active_backend` or `shadow_backend`.
   Each role must contain exactly one backend. No changes to `shadowbackend` are
   needed to add a registered implementation.

For example, a custom active backend can be paired with testfs:

```yaml
backends:
  - namespace: .*
    backend:
      shadow:
        active_backend:
          custom:
            endpoint: "https://storage.example.com"
        shadow_backend:
          testfs:
            addr: localhost:7357
            root: tags
            name_path: docker_tag
```

The `endpoint` field is illustrative; the custom factory defines its own configuration
schema. Both child factories receive the complete `backend.AuthConfig`, the shadow
backend's metrics scope, and its logger. Each factory selects its authentication
settings by registered name, such as `auth["custom"]`.

The authentication map must contain entries for both registered names, even when a
backend does not require credentials. An empty entry is sufficient if its factory
accepts it. A name that has not been registered fails with
`no backend client defined with name <name>`.
