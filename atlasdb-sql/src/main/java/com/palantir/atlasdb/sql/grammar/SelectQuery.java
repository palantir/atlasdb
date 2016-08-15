package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.columns.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.columns.JdbcComponentMetadata;
import com.palantir.atlasdb.sql.jdbc.results.columns.QueryColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.parsed.ParsedRowResult;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(TableMetadata metadata,
                                     AtlasSQLParser.Select_queryContext query) throws SQLException {
        LinkedHashMap<String, AggregationType> aggregatedColNames = createSelectedColumnsIndex(query.column_clause());
        LinkedHashMap<String, JdbcColumnMetadata> allCols = makeAllColumnsIndex(metadata);
        List<QueryColumnMetadata> columns = makeSelectedColumns(allCols, aggregatedColNames);
        WhereClause where = WhereClause.create(
                query.where_clause(),
                columns.stream().map(QueryColumnMetadata::getMetadata).collect(Collectors.toList()));
        return ImmutableSelectQuery.builder()
                .table(getTableName(query))
                .rangeRequest(RangeRequest.all())
                .columns(columns)
                .labelOrNameToMetadata(allCols)
                .prefilterConstraint(where.prefilterConstraints())
                .postfilterPredicate(where.postfilterPredicate())
                .build();
    }

    private static LinkedHashMap<String, AggregationType> createSelectedColumnsIndex(AtlasSQLParser.Column_clauseContext columnCtx) {
        if (columnCtx.column_list() != null && columnCtx.all_columns() == null) {
            return columnCtx.column_list().column_or_aggregate()
                    .stream()
                    .map(SelectQuery::parseAggregate)
                    .collect(
                            Collectors.toMap(
                                    Pair::getLeft,
                                    Pair::getRight,
                                    (k1, k2) -> k1,
                                    Maps::newLinkedHashMap));
        }
        return Maps.newLinkedHashMap();
    }

    private static Pair<String, AggregationType> parseAggregate(AtlasSQLParser.Column_or_aggregateContext column) {
        return column.aggregate_column() == null
                ? Pair.of(column.column_name().getText(), AggregationType.IDENTITY)
                : Pair.of(column.aggregate_column().column_name().getText(),
                AggregationType.from(column.aggregate_column().aggregate_function().getText()));
    }

    public static String getTableName(AtlasSQLParser.Select_queryContext query) {
        return query.table_reference().getText();
    }

    private static LinkedHashMap<String, JdbcColumnMetadata> makeAllColumnsIndex(TableMetadata metadata) {
        LinkedHashMap<String, JdbcColumnMetadata> allCols = Maps.newLinkedHashMap();
        allCols.putAll(metadata.getRowMetadata().getRowParts()
                               .stream()
                               .collect(Collectors.toMap(NameComponentDescription::getComponentName,
                                                         JdbcComponentMetadata.RowComp::new,
                                                         throwingMerger(),
                                                         LinkedHashMap::new)));
        if (metadata.getColumns().getNamedColumns() != null) {
            allCols.putAll(metadata.getColumns().getNamedColumns()
                                   .stream()
                                   .collect(Collectors.toMap(NamedColumnDescription::getShortName,
                                                             JdbcComponentMetadata.NamedCol::new,
                                                             throwingMerger(),
                                                             LinkedHashMap::new)));
        }
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.putAll(metadata.getColumns().getDynamicColumn().getColumnNameDesc().getRowParts()
                                   .stream()
                                   .collect(Collectors.toMap(NameComponentDescription::getComponentName,
                                                             JdbcComponentMetadata.ColComp::new,
                                                             throwingMerger(),
                                                             LinkedHashMap::new)));
            allCols.put(JdbcComponentMetadata.ValueCol.VALUE_COLUMN_LABEL,
                        new JdbcComponentMetadata.ValueCol(metadata.getColumns()
                                                                   .getDynamicColumn()
                                                                   .getValue()));
        }
        return allCols;
    }

    private static List<QueryColumnMetadata> makeSelectedColumns(Map<String, JdbcColumnMetadata> allCols,
                                                                 LinkedHashMap<String, AggregationType> aggregatedColNames) {
        // select only given column names
        if (!aggregatedColNames.isEmpty()) {
            return allCols.entrySet().stream()
                    .map(entry -> createSelectedColumn(entry, aggregatedColNames))
                    .collect(Collectors.toList());
        }
        // select all columns
        return allCols.values().stream()
                            .map(c -> new QueryColumnMetadata(c, AggregationType.IDENTITY))
                            .collect(Collectors.toList());
    }

    private static QueryColumnMetadata createSelectedColumn(Map.Entry<String, JdbcColumnMetadata> entry,
                                                            LinkedHashMap<String, AggregationType> aggregatedColNames) {
        String col = entry.getKey();
        JdbcColumnMetadata metadata = entry.getValue();
        if (aggregatedColNames.containsKey(col)) {
            return new QueryColumnMetadata(metadata, aggregatedColNames.get(col));
        }
        return new QueryColumnMetadata(metadata);
    }

    // Utility copied from Collectors to allow collecting map types.
    private static <T> BinaryOperator<T> throwingMerger() {
        return (u,v) -> { throw new IllegalStateException(String.format("Duplicate key %s", u)); };
    }

    @Value.Derived
    public TableRange tableRange() {
        List<byte[]> cols = columns()
                .stream()
                .filter(QueryColumnMetadata::isSelected)
                .map(QueryColumnMetadata::getMetadata)
                .filter(JdbcColumnMetadata::isNamedCol)
                .map(c -> c.getName().getBytes())
                .collect(Collectors.toList());
        RangeRequest.Builder rangeBuilder = RangeRequest.builder();
        if (prefilterConstraint().getLowerBound() != null) {
            rangeBuilder.startRowInclusive(prefilterConstraint().getLowerBound());
        }
        if (prefilterConstraint().getUpperBound() != null) {
            rangeBuilder.endRowExclusive(prefilterConstraint().getUpperBound());
        }
        RangeRequest range = rangeBuilder.build();
        return new TableRange(table(),
                range.getStartInclusive(),
                range.getEndExclusive(),
                cols,
                batchSize());
    }

    public abstract String table();
    public abstract RangeRequest rangeRequest();
    public abstract List<QueryColumnMetadata> columns();
    public abstract LinkedHashMap<String, JdbcColumnMetadata> labelOrNameToMetadata();
    public abstract RowComponentConstraint prefilterConstraint();
    public abstract Predicate<ParsedRowResult> postfilterPredicate();

    @Value.Default
    public int batchSize() {
        return 2000;
    }

}
