package com.palantir.atlasdb.keyvalue.rdbms;

import java.io.IOException;

import org.postgresql.jdbc2.optional.PoolingDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class PostgresAtlasDbFactory implements AtlasDbFactory<PostgresKeyValueService> {

    @Override
    public String getType() {
        return "postgres";
    }

    @Override
    public PostgresKeyValueService createRawKeyValueService(JsonNode config)
            throws IOException {
        JsonNode postgresConfig = config.get("postgresConfig");
        String host = postgresConfig.get("host").asText();
        int port = postgresConfig.get("port").asInt();
        String db = postgresConfig.get("db").asText();
        String user = postgresConfig.get("user").asText();
        String password = postgresConfig.get("password").asText();

        PoolingDataSource ds = new PoolingDataSource();
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setDatabaseName(db);
        ds.setUser(user);
        ds.setPassword(password);

        return new PostgresKeyValueService(ds, PTExecutors.newCachedThreadPool());
    }

    @Override
    public TimestampService createTimestampService(
            PostgresKeyValueService rawKvs) {
        return new InMemoryTimestampService();
    }

}
