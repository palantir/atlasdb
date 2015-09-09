package com.palantir.atlasdb.keyvalue.rdbms;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.common.annotation.Immutable;

@Immutable public final class PostgresKeyValueConfiguration {

    @Nonnull public final String host;
    @Nonnull public final int port;
    @Nonnull public final String db;
    @Nonnull public final String user;
    @Nonnull public final String password;

    @JsonCreator
    public PostgresKeyValueConfiguration(
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("db") String db,
            @JsonProperty("user") String user,
            @JsonProperty("password") String password) {

        this.host = host;
        this.port = port;
        this.db = db;
        this.user = user;
        this.password = password;
    }

}
