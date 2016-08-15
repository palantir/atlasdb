package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.JdbcComponentMetadata;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;
import com.palantir.atlasdb.sql.jdbc.results.SelectableJdbcColumnMetadata;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(TableMetadata metadata,
                                     AtlasSQLParser.Select_queryContext query) throws SQLException {

        LinkedHashMap<String, ColumnOrAggregateColumn> selectedColumnsIndex = Maps.newLinkedHashMap();
        if (query.column_clause().column_list() != null && query.column_clause().all_columns() == null) {
            final List<ColumnOrAggregateColumn> selectedColumns = query.column_clause()
                                                                       .column_list()
                                                                       .column_or_aggregate()
                                                                       .stream()
                                                                       .map(SelectQuery::parseAggregate)
                                                                       .collect(Collectors.toList());
            // Sadly, for some reason this cannot be streamed: "cannot reference ::getColumnName() from static context" :(
            for (ColumnOrAggregateColumn columnOrAggregateColumn : selectedColumns) {
                selectedColumnsIndex.put(columnOrAggregateColumn.getColumnName(), columnOrAggregateColumn);
            }
        } else {
            // * means selectedColumns are empty.
        }
        Map<String, JdbcColumnMetadata> allCols = makeAllColumnsIndex(metadata);
        List<SelectableJdbcColumnMetadata> columns = makeSelectedColumns(allCols, selectedColumnsIndex);
        WhereClause where = WhereClause.create(
                query.where_clause(),
                columns.stream().map(SelectableJdbcColumnMetadata::getMetadata).collect(Collectors.toList()));
        return ImmutableSelectQuery.builder()
                .table(getTableName(query))
                .rangeRequest(RangeRequest.all())
                .columns(columns)
                .labelOrNameToMetadata(allCols)
                .prefilterConstraint(where.prefilterConstraints())
                .postfilterPredicate(where.postfilterPredicate())
                .build();
    }

    private static ColumnOrAggregateColumn parseAggregate(AtlasSQLParser.Column_or_aggregateContext column) {
        return column.aggregate_column() == null
                ? new ColumnOrAggregateColumn(column.column_name().getText(), AggregateFunction.IDENTITY)
                : new ColumnOrAggregateColumn(column.aggregate_column().column_name().getText(),
                                                              AggregateFunction.parse(column.aggregate_column().aggregate_function()));
    }

    public static String getTableName(AtlasSQLParser.Select_queryContext query) {
        return query.table_reference().getText();
    }

    private static ImmutableMap<String, JdbcColumnMetadata> makeAllColumnsIndex(TableMetadata metadata) {
        ImmutableMap.Builder<String, JdbcColumnMetadata> allCols = ImmutableMap.builder();
        allCols.putAll(metadata.getRowMetadata().getRowParts()
                               .stream()
                               .collect(Collectors.toMap(NameComponentDescription::getComponentName,
                                                         JdbcComponentMetadata.RowComp::new)));
        if (metadata.getColumns().getNamedColumns() != null) {
            allCols.putAll(metadata.getColumns().getNamedColumns()
                                   .stream()
                                   .collect(Collectors.toMap(NamedColumnDescription::getShortName, JdbcComponentMetadata.NamedCol::new)));
        }
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.putAll(metadata.getColumns().getDynamicColumn().getColumnNameDesc().getRowParts()
                                   .stream()
                                   .collect(Collectors.toMap(NameComponentDescription::getComponentName, JdbcComponentMetadata.ColComp::new)));
            allCols.put(JdbcComponentMetadata.ValueCol.VALUE_COLUMN_LABEL,
                        new JdbcComponentMetadata.ValueCol(metadata.getColumns()
                                                                   .getDynamicColumn()
                                                                   .getValue()));
        }
        return allCols.build();
    }

    private static List<SelectableJdbcColumnMetadata> makeSelectedColumns(Map<String, JdbcColumnMetadata> allCols,
                                                                          LinkedHashMap<String, ColumnOrAggregateColumn> selectColNames) {


        // select only given column names
        if (!selectColNames.isEmpty()) {
            return selectColNames.entrySet().stream()
                                 .map(entry -> new SelectableJdbcColumnMetadata(allCols.get(entry.getKey()),
                                                                                entry.getValue().getAggregateFunction()))
                    .collect(Collectors.toList());
        }
        // select all columns
        return allCols.values().stream()
                            .map(c -> new SelectableJdbcColumnMetadata(c, AggregateFunction.IDENTITY))
                            .collect(Collectors.toList());
    }

    @Value.Derived
    public TableRange tableRange() {
        List<byte[]> cols = columns()
                .stream()
                .filter(SelectableJdbcColumnMetadata::isSelected)
                .map(SelectableJdbcColumnMetadata::getMetadata)
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
    public abstract List<SelectableJdbcColumnMetadata> columns();
    public abstract Map<String, JdbcColumnMetadata> labelOrNameToMetadata();
    public abstract RowComponentConstraint prefilterConstraint();
    public abstract Predicate<ParsedRowResult> postfilterPredicate();

    @Value.Default
    public int batchSize() {
        return 2000;
    }

    public static class ColumnOrAggregateColumn {
        String columnName;
        AggregateFunction aggregateFunction;

        public ColumnOrAggregateColumn(String columnName, AggregateFunction aggregateFunction) {
            this.columnName = columnName;
            this.aggregateFunction = aggregateFunction;
        }

        public String getColumnName() {
            return columnName;
        }

        public AggregateFunction getAggregateFunction() {
            return aggregateFunction;
        }
    }

}
