package com.palantir.atlasdb.sql.jdbc.results;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import com.palantir.atlasdb.table.description.ValueType;

class Aggregator implements BinaryOperator<ParsedRowResult> {
    private final List<SelectableJdbcColumnMetadata> selectedCols;
    private List<TypedAggregatedColumn> aggregatedCols;

    public Aggregator(List<SelectableJdbcColumnMetadata> selectedCols) {
        this.selectedCols = selectedCols;
        aggregatedCols =
                selectedCols.stream()
                            .filter(SelectableJdbcColumnMetadata::isAggregate)
                            .map(TypedAggregatedColumn::new)
                            .collect(Collectors.toList());
    }

    @Override
    public ParsedRowResult apply(ParsedRowResult result1, ParsedRowResult result2) {
        final List<JdbcColumnMetadataAndValue> results
                = aggregatedCols
                .stream()
                .map(col -> {
                    try {
                        final int i = result1.getIndexFromColumnLabel(col.metadata.getMetadata()
                                                                                  .getLabel());
                        final Object o1 = result1.get(i, JdbcReturnType.OBJECT);
                        final Object o2 = result2.get(i, JdbcReturnType.OBJECT);
                        final ValueType valueType = col.metadata.getMetadata().getValueType();
                        switch (valueType) {
                            case VAR_LONG:
                            case VAR_SIGNED_LONG:
                            case FIXED_LONG:
                            case FIXED_LONG_LITTLE_ENDIAN:
                                return new JdbcColumnMetadataAndValue(col.metadata.getMetadata(),
                                                                      valueType.convertFromJava(
                                                                              col.accumulator.apply(o1, o2)));
                            case SHA256HASH:
                            case VAR_STRING:
                            case STRING:
                            case BLOB:
                            case SIZED_BLOB:
                            case NULLABLE_FIXED_LONG:
                            case UUID:
                                throw new ParsedRowResult.AggregatorException("Unsupported type " + valueType);
                        }
                    } catch (SQLException e) {
                        throw new ParsedRowResult.AggregatorException(e);
                    }
                    return null;
                })
                .collect(Collectors.toList());

        return new ParsedRowResult(results);

    }

    public ParsedRowResult empty() {
       return new ParsedRowResult(selectedCols.stream()
               .map(col -> {
                   final ValueType valueType = col.getMetadata().getValueType();
                   switch(valueType) {

                       case VAR_LONG:
                       case VAR_SIGNED_LONG:
                       case FIXED_LONG:
                       case FIXED_LONG_LITTLE_ENDIAN:
                           switch (col.getAggregateFunction()) {
                               case NULL:
                               case IDENTITY:
                                   break;
                               case COUNT:
                                   return new JdbcColumnMetadataAndValue(col.getMetadata(), valueType.convertFromJava(0L));
                               case MAX:
                                   return new JdbcColumnMetadataAndValue(col.getMetadata(), valueType.convertFromJava(Long.MIN_VALUE));
                               case MIN:
                                   return new JdbcColumnMetadataAndValue(col.getMetadata(), valueType.convertFromJava(Long.MAX_VALUE));
                           }
                       case VAR_STRING:
                       case STRING:
                       case SHA256HASH:
                       case BLOB:
                       case SIZED_BLOB:
                       case NULLABLE_FIXED_LONG:
                       case UUID:
                           throw new ParsedRowResult.AggregatorException("Unsupported type for aggregation.");
                   }
                   return null;
               }).collect(Collectors.toList()));
    }

    public boolean toAggregate() {
        return !aggregatedCols.isEmpty();
    }

    /** Pre-create the accumulators and identity values for aggregating columns
     */
    private static class TypedAggregatedColumn {
        SelectableJdbcColumnMetadata metadata;
        BinaryOperator<Object> accumulator;

        public TypedAggregatedColumn(SelectableJdbcColumnMetadata metadata) {
            this.metadata = metadata;
            switch (metadata.getAggregateFunction()) {

                case NULL:
                    throw new ParsedRowResult.AggregatorException("Unsupported operation");
                case IDENTITY:
                    break;
                case COUNT:
                    accumulator = new BinaryOperator<Object>() {
                        @Override
                        public Object apply(Object o, Object o2) {
                            return null;
                        }
                    };
                    break;
                case MAX:
                    switch (metadata.getMetadata().getValueType()) {

                        case VAR_LONG:
                        case VAR_SIGNED_LONG:
                        case FIXED_LONG:
                        case FIXED_LONG_LITTLE_ENDIAN:
                        case NULLABLE_FIXED_LONG:
                            accumulator = new BinaryOperator<Object>() {
                                @Override
                                public Object apply(Object o, Object o2) {
                                    return null;
                                }
                            };
                            break;

                        case VAR_STRING:
                        case STRING:

                            break;
                        case SHA256HASH:
                            break;
                        case BLOB:
                            break;
                        case SIZED_BLOB:
                            break;
                        case UUID:
                            break;
                    }

                    break;
                case MIN:
                    accumulator = new BinaryOperator<Object>() {
                        @Override
                        public Object apply(Object o, Object o2) {
                            return null;
                        }
                    };
                    break;
            }
        }

    }
}
