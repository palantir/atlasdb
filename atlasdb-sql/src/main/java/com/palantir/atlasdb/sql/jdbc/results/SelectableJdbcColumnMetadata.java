package com.palantir.atlasdb.sql.jdbc.results;

import com.palantir.atlasdb.sql.grammar.AggregateFunction;

public class SelectableJdbcColumnMetadata {

    protected final JdbcColumnMetadata metadata;
    private final AggregateFunction aggregateFunction;

    public SelectableJdbcColumnMetadata(JdbcColumnMetadata metadata, AggregateFunction aggregateFunction) {
        this.metadata = metadata;
        this.aggregateFunction = aggregateFunction;
    }

    public JdbcColumnMetadata getMetadata() {
        return metadata;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public boolean isSelected() {
        return aggregateFunction != AggregateFunction.NULL;
    }

    public boolean isAggregate() {
        return aggregateFunction != AggregateFunction.NULL && aggregateFunction != AggregateFunction.IDENTITY;
    }
}
