package com.palantir.atlasdb.sql.jdbc.results.columns;

import java.util.Optional;

import com.palantir.atlasdb.sql.grammar.AggregationType;

public class QueryColumnMetadata {

    protected final JdbcColumnMetadata metadata;
    private final Optional<AggregationType> aggregateType;

    public QueryColumnMetadata(JdbcColumnMetadata metadata, AggregationType aggregationType) {
        this.metadata = metadata;
        this.aggregateType = Optional.of(aggregationType);
    }

    public QueryColumnMetadata(JdbcColumnMetadata metadata) {
        this.metadata = metadata;
        this.aggregateType = Optional.empty();
    }

    public JdbcColumnMetadata getMetadata() {
        return metadata;
    }

    public AggregationType getAggregationType() {
        return aggregateType.get();
    }

    public boolean isSelected() {
        return aggregateType.isPresent();
    }

}
