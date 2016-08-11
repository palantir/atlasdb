package com.palantir.atlasdb.sql.jdbc.statement;

import java.sql.ResultSet;

import org.immutables.value.Value;

@Value.Immutable
public abstract class StatementConfig {

    @Value.Default
    public int maxRows() {
        return 0; // unlimited
    }

    @Value.Default
    public int maxFieldSize() {
        return 0; // unlimited
    }

    @Value.Default
    public int fetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

}
