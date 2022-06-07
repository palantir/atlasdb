AtlasDB Configuration Support
=============================
This project provides convenience methods for initializing `TransactionManager`s
and leader election systems for running your own HA server setups.

It includes a configuration format that enables you to add a typed configuration
block to systems like Dropwizard apps that accept such configuration objects.

Initialization
--------------
Creating a `TransactionManager`:

```java
AtlasDbConfig atlasConfig = ...
Schema atlasSchema = ...
SerializableTransactionManager tm = TransactionManagers.builder()
    .config(atlasConfig)
    .schemas(ImmutableSet.of(atlasSchema))
    .userAgent("productName (productVersion)")
    .buildSerializable();
```

The last item is a consumer of resources meant to be exposed to as web
endpoints, and expects something akin to Dropwizard's
`environment.jersey().register(Object)` method in order to support addition
of necessary web endpoints for leader-based time and lock services.

Dropwizard
----------
For Dropwizard apps, simply add an `AtlasDbConfig` type to your existing
`Configuration` class, e.g.:

```java
public final class MyAppConfiguration extends Configuration {
    private final AtlasDbConfig atlas;
    public MyAppConfiguration(@JsonProperty("atlas") AtlasDbConfig atlas) {
        this.atlas = atlas;
    }
    public AtlasDbConfig getAtlas() {
        return atlas;
    }
}
```

And initialization code to your run method:

```java
public void run(AtlasDbServerConfiguration config, Environment env) throws Exception {
    TransactionManager transactionManager = TransactionManagers.builder()
        .config(config.getAtlas())
        .registrar(env.jersey()::register)
        .userAgent("productName (productVersion)")
        .buildSerializable();
    ...
```

Which will enable the following config object:

```yaml
# AtlasDB Configuration
# atlas:
#   keyValueService: (configuration for the storage backend)
#     type:          (postgres|cassandra) type of key value store to use
#     # cassandra specific config
#     ssl:           (true|false) true to use SSL
#     mutationBatchCount: (optional, default 5000)
#     mutationBatchSizeBytes: (optional, default 4*1024*1024)
#     fetchBatchCount:    (optional, default 5000)
#  lock:             (configuration of the lock client; omit for embedded mode)
#    servers:        a list of available lock servers
#      - [uri]       e.g. https://localhost:8101/api/
#  timestamp:        (configuration of the timestamp client; omit for embedded mode)
#    servers:        a list of available lock servers
#      - [uri]       e.g. https://localhost:8101/api/
#  leader:           (optional) leader configuration allows running lock-stamp
#                    services in a cluster, and should be used when Jobs Service
#                    will be run in a clustered mode
#                    regardless of configuration, AtlasDB will respect the lock
#                    and timestamp client configurations above
#                    when configured, the embedded lock and timestamp servers
#                    in this process will use the specified leader election
#                    criteria to determine if the embedded process should
#                    respond to requests
#    quorumSize:     quorum requirement, must be a majority
#    localServer:    the URI of this node
#    leaders:        a list of potential leaders
#      - [uri]       e.g. https://loclahost:8101/api/
#    learnerLogDir:  (optional, default var/data/paxos/learner) the persistence
#                    directory for the learner, must actually persist between
#                    executions of the server
#    acceptorLogDir: (optional, default var/data/paxos/acceptor) the persistence
#                    directory for the acceptor, must actually persist between
#                    executions of the server
#atlasDbRuntime:
#    keyValueService:
#        type: cassandra
#        servers:       a list of cassandra nodes to use
#         - [hostname] e.g. localhost
#        replicationFactor: replication factor


# Example: a default configuration for running a single process Jobs Service
# backed by a single node Cassandra instance and using the embedded
# lock-stamp services:
# atlas:
#   keyValueService:
#     type: cassandra
#     ssl: false

# atlasDbRuntime:
#    keyValueService:
#      type: cassandra
#      servers:
#        - localhost:9160
#      replicationFactor: 1
```
