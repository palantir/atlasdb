package com.palantir.atlasdb.sql.jdbc.results.aggregate;

import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.sql.grammar.AggregationType;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.parsed.IterableParsedRowResults;
import com.palantir.atlasdb.sql.jdbc.results.parsed.LocalParsedRowResultsIterator;
import com.palantir.atlasdb.sql.jdbc.results.parsed.ParsedRowResultsIterator;
import com.palantir.atlasdb.table.description.ValueType;

public class Aggregators {

    public static ParsedRowResultsIterator aggreate(IterableParsedRowResults results,
                                                    List<QueryColumnMetadata> columns) {
        if (anyAggregators(columns)) {
            final Aggregator aggregator = Aggregator.create(columns);
            return new LocalParsedRowResultsIterator(
                    Lists.newArrayList(
                            StreamSupport.stream(results.spliterator(), false)
                                    .reduce(aggregator.empty(), aggregator))
                            .iterator());
        } else {
            return results.iterator();
        }
    }

    private static boolean anyAggregators(List<QueryColumnMetadata> columns) {
        return columns.stream().anyMatch(c -> c.isSelected() && c.getAggregationType() != AggregationType.IDENTITY);
    }

    static BinaryOperator getBinaryOperatorFor(AggregationType aType, ValueType vType) {
        switch (aType) {
            case IDENTITY:
                throw new UnsupportedOperationException(
                        "Cannot select aggregated and non-aggregated columns without a GROUP BY clause " +
                                "(currently GROUP BY is not supported).");
            case COUNT:
                return (o1, o2) -> (Long) o1 + 1L;
            case MAX:
                switch (vType) {
                    case VAR_LONG:
                    case VAR_SIGNED_LONG:
                    case FIXED_LONG:
                    case FIXED_LONG_LITTLE_ENDIAN:
                    case NULLABLE_FIXED_LONG:
                        return (o1, o2) -> Math.max((Long)o1, (Long)o2);
                    case SHA256HASH:
                    case VAR_STRING:
                    case STRING:
                    case BLOB:
                    case SIZED_BLOB:
                    case UUID:
                        break;
                }
                break;
            case MIN:
                switch (vType) {
                    case VAR_LONG:
                    case VAR_SIGNED_LONG:
                    case FIXED_LONG:
                    case FIXED_LONG_LITTLE_ENDIAN:
                    case NULLABLE_FIXED_LONG:
                        return (o1, o2) -> Math.min((Long)o1, (Long)o2);
                    case SHA256HASH:
                    case VAR_STRING:
                    case STRING:
                    case BLOB:
                    case SIZED_BLOB:
                    case UUID:
                        break;
                }
                break;
        }
        throw new UnsupportedOperationException(String.format("Cannot aggregate type '%s' with '%s'.", vType, aType));
    }

}
