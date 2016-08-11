package com.palantir.atlasdb.sql.jdbc.statement;

import java.sql.ResultSet;
import java.util.Optional;

import com.google.common.base.Preconditions;

public class SqlExecutionResult {
    // jdbc uses -1 to indicate no updates
    public static final int NO_UPDATES = -1;

    private final int updateCount;
    private final Optional<ResultSet> resultSet;

    public static SqlExecutionResult fromResult(ResultSet resultSet) {
        return new SqlExecutionResult(Optional.of(resultSet), NO_UPDATES);
    }

    public static SqlExecutionResult fromUpdate(int updateCount) {
        Preconditions.checkState(updateCount >= 0, "update count must be non-negative");
        return new SqlExecutionResult(Optional.empty(), updateCount);
    }

    public static SqlExecutionResult empty() {
        return new SqlExecutionResult(Optional.empty(), NO_UPDATES);
    }

    private SqlExecutionResult(Optional<ResultSet> resultSet, int updateCount) {
        this.updateCount = updateCount;
        this.resultSet = resultSet;
    }

    public Optional<ResultSet> resultSet() {
        return resultSet;
    }

    public int updateCount() {
        return updateCount;
    }

}
