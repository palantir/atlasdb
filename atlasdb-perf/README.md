# AtlasDB Performance CLI

The AtlasDB Performance CLI is an extendable framework for creating and running performance benchmarks against AtlasDB services.

## Running Tips

`//TODO`

1. To run with the Cassandra backend, you must first create the docker image by running the following command:

        cd $ATLASDB_ROOT/docker-containers/cassandra
        docker build -t atlas-cassandra:2.2-v0.1 .
