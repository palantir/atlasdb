package com.palantir.atlasdb.keyvalue.rdbms;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.palantir.atlasdb.spi.AtlasDbFactory;
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
        return PostgresKeyValueService.create(createConfig(config.get("postgresConfig")));
    }

    @Override
    public TimestampService createTimestampService(
            PostgresKeyValueService rawKvs) {
        return new InMemoryTimestampService();
    }

    private PostgresKeyValueConfiguration createConfig(JsonNode node) {
        String host = node.get("host").asText();
        int port = node.get("port").asInt();
        String db = node.get("db").asText();
        String user = node.get("user").asText();
        String password = node.get("password").asText();

        return new PostgresKeyValueConfiguration(host, port, db, user, password);
    }

}
