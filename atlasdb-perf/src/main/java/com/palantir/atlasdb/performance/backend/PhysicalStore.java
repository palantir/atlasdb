package com.palantir.atlasdb.performance.backend;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * The physical store backing the AtlasDB key-value service. Any new physical store should extend this class (currently only Postgres is
 * implemented).
 */
public abstract class PhysicalStore implements AutoCloseable {

    public abstract KeyValueService connect();

    public static PhysicalStore create(PhysicalStore.Type type) {
        switch (type) {
            case POSTGRES:
                return new PostgresPhysicalStore();
            case ORACLE:
            case CASSANDRA:
                throw new NotImplementedException();
        }
        throw new NotImplementedException();
    }

    static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    public enum Type {
        POSTGRES,
        ORACLE,
        CASSANDRA,
    }

}
