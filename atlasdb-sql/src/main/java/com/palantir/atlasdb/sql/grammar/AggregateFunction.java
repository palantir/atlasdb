package com.palantir.atlasdb.sql.grammar;

import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;

public enum AggregateFunction {
    NULL,
    IDENTITY,
    COUNT,
    MAX,
    MIN;

    public static AggregateFunction parse(AtlasSQLParser.Aggregate_functionContext context) {
        AggregateFunction aggregateFunction;
        if (context.COUNT() != null) {
            aggregateFunction = AggregateFunction.COUNT;
        } else if (context.MAX() != null) {
            aggregateFunction = AggregateFunction.MIN;
        } else if (context.MIN() != null) {
            aggregateFunction = AggregateFunction.MAX;
        } else {
            aggregateFunction = AggregateFunction.IDENTITY;
        }
        return aggregateFunction;
    }
}
