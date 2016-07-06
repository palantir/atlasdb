package com.palantir.atlasdb.performance.backend;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Postgres backed physical store for the AtlasDb key-value service.
 *
 * @author mwakerman
 */
public class PostgresPhysicalStore extends PhysicalStore {

    private static DockerComposition composition;

    @Override
    public KeyValueService connect() {
        composition = dockerCompose();

        DockerPort port = composition.hostNetworkedPort(PhysicalStoreConfig.POSTGRES_PORT_NUMBER);
        InetSocketAddress address = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ImmutablePostgresConnectionConfig connectionConfig = ImmutablePostgresConnectionConfig.builder()
                .dbName(PhysicalStoreConfig.POSTGRES_DB_NAME)
                .dbLogin(PhysicalStoreConfig.POSTGRES_USER_LOGIN)
                .dbPassword(PhysicalStoreConfig.POSTGRES_USER_PASSWORD)
                .host(address.getHostName())
                .port(address.getPort())
                .build();

        ImmutableDbKeyValueServiceConfig conf = ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutablePostgresDdlConfig.builder().build())
                .build();

        return ConnectionManagerAwareDbKvs.create(conf);
    }

    private DockerComposition dockerCompose() {
        File dockerComposeFile;
        try {
            dockerComposeFile = PhysicalStoreUtils.writeResourceToTempFile(
                    PostgresPhysicalStore.class, PhysicalStoreConfig.POSTGRES_DOCKER_COMPOSE_PATH);
        } catch (IOException e) {
            throw new RuntimeException("Could not write docker compose resource to file.", e);
        }

        DockerComposition comp = DockerComposition.of(dockerComposeFile.getAbsolutePath())
                .waitingForHostNetworkedPort(PhysicalStoreConfig.POSTGRES_PORT_NUMBER, toBeOpen())
                .saveLogsTo(PhysicalStoreConfig.POSTGRES_DOCKER_LOGS_DIR)
                .build();

        try {
            comp.before();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return comp;
    }

    @Override
    public void close() throws Exception {
        composition.after();
    }
}
