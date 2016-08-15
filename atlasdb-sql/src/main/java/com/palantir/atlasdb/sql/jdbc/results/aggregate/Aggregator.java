package com.palantir.atlasdb.sql.jdbc.results.aggregate;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import com.palantir.atlasdb.sql.jdbc.results.JdbcReturnType;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumn;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.parsed.ParsedRowResult;
import com.palantir.atlasdb.table.description.ValueType;

public class Aggregator implements BinaryOperator<ParsedRowResult> {
    private final LinkedHashMap<QueryColumnMetadata, BinaryOperator> colToAggregate;

    public static Aggregator create(List<QueryColumnMetadata> columnMetadatas) {
        LinkedHashMap<QueryColumnMetadata, BinaryOperator> colsToAggregator =
                columnMetadatas.stream()
                        .filter(QueryColumnMetadata::isSelected)
                        .collect(
                                Collectors.toMap(
                                        c -> c,
                                        c -> Aggregators.getBinaryOperatorFor(c.getAggregationType(), c.getMetadata().getValueType()),
                                        (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); },
                                        LinkedHashMap::new));
        return new Aggregator(colsToAggregator);
    }

    public Aggregator(LinkedHashMap<QueryColumnMetadata, BinaryOperator> colToAggregate) {
        this.colToAggregate = colToAggregate;
    }

    @Override
    public ParsedRowResult apply(ParsedRowResult result1, ParsedRowResult result2) {
        final List<QueryColumn> results = colToAggregate.entrySet().stream()
                .map(entry -> {
                    try {
                        QueryColumnMetadata colMeta = entry.getKey();
                        BinaryOperator bop = entry.getValue();
                        final int i = result1.getIndexFromColumnLabel(colMeta.getMetadata().getLabel());
                        final Object o1 = result1.get(i, JdbcReturnType.OBJECT);
                        final Object o2 = result2.get(i, JdbcReturnType.OBJECT);
                        final ValueType valueType = colMeta.getMetadata().getValueType();
                        return QueryColumn.create(colMeta,
                                valueType.convertFromJava(bop.apply(o1, o2)));
                    } catch (SQLException e) {
                        throw new ParsedRowResult.AggregatorException(e);
                    }
                })
                .collect(Collectors.toList());

        return ParsedRowResult.create(results);
    }

    ParsedRowResult empty() {
        return ParsedRowResult.create(colToAggregate.entrySet().stream()
                .map(entry -> {
                    QueryColumnMetadata colMeta = entry.getKey();
                    final ValueType valueType = colMeta.getMetadata().getValueType();
                    switch (colMeta.getAggregationType()) {
                        case IDENTITY:
                            break;
                        case COUNT:
                            return QueryColumn.create(colMeta, ValueType.FIXED_LONG.convertFromJava(0L));
                        case MAX:
                            switch(valueType) {
                                case VAR_LONG:
                                case VAR_SIGNED_LONG:
                                case FIXED_LONG:
                                case FIXED_LONG_LITTLE_ENDIAN:
                                case NULLABLE_FIXED_LONG:
                                    return QueryColumn.create(colMeta, valueType.convertFromJava(Long.MIN_VALUE));
                                case VAR_STRING:
                                case STRING:
                                case SHA256HASH:
                                case BLOB:
                                case SIZED_BLOB:
                                case UUID:
                                    break;
                            }
                            break;
                        case MIN:
                            switch(valueType) {
                                case VAR_LONG:
                                case VAR_SIGNED_LONG:
                                case FIXED_LONG:
                                case FIXED_LONG_LITTLE_ENDIAN:
                                case NULLABLE_FIXED_LONG:
                                    return QueryColumn.create(colMeta, valueType.convertFromJava(Long.MAX_VALUE));
                                case VAR_STRING:
                                case STRING:
                                case SHA256HASH:
                                case BLOB:
                                case SIZED_BLOB:
                                case UUID:
                                    break;
                            }
                            break;
                    }
                    switch(valueType) {
                        case VAR_LONG:
                        case VAR_SIGNED_LONG:
                        case FIXED_LONG:
                        case FIXED_LONG_LITTLE_ENDIAN:
                        case VAR_STRING:
                        case STRING:
                        case SHA256HASH:
                        case BLOB:
                        case SIZED_BLOB:
                        case NULLABLE_FIXED_LONG:
                        case UUID:
                    }
                    throw new ParsedRowResult.AggregatorException("Unsupported type for aggregation.");
                }).collect(Collectors.toList()));
    }

}
