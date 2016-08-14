package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.RuleContext;
import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.JdbcComponentMetadata;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;
import com.palantir.atlasdb.sql.jdbc.results.SelectableJdbcColumnMetadata;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(TableMetadata metadata,
                                     AtlasSQLParser.Select_queryContext query) throws SQLException {
        Optional<Set<String>> selectColumnNames =  query.column_clause().column_list() == null
                ? Optional.empty()
                : Optional.of(
                query.column_clause().column_list().column_name()
                        .stream()
                        .map(RuleContext::getText)
                        .collect(Collectors.toSet()));
        List<SelectableJdbcColumnMetadata> columns = makeColumns(metadata, selectColumnNames);
        WhereClause where = WhereClause.create(
                query.where_clause(),
                columns.stream().map(SelectableJdbcColumnMetadata::getMetadata).collect(Collectors.toList()));
        return ImmutableSelectQuery.builder()
                .table(getTableName(query))
                .rangeRequest(RangeRequest.all())
                .columns(columns)
                .prefilterConstraint(where.prefilterConstraints())
                .postfilterPredicate(where.postfilterPredicate())
                .build();
    }

    public static String getTableName(AtlasSQLParser.Select_queryContext query) {
        return query.table_reference().getText();
    }

    private static List<SelectableJdbcColumnMetadata> makeColumns(TableMetadata metadata, Optional<Set<String>> selectColNames) {
        ImmutableList.Builder<JdbcColumnMetadata> allCols = ImmutableList.builder();
        allCols.addAll(metadata.getRowMetadata().getRowParts()
                .stream()
                .map(JdbcComponentMetadata.RowComp::new)
                .collect(Collectors.toList()));
        if (metadata.getColumns().getNamedColumns() != null) {
            allCols.addAll(metadata.getColumns().getNamedColumns()
                    .stream()
                    .map(JdbcComponentMetadata.NamedCol::new)
                    .collect(Collectors.toList()));
        }
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.addAll(metadata.getColumns().getDynamicColumn().getColumnNameDesc().getRowParts()
                    .stream()
                    .map(JdbcComponentMetadata.ColComp::new)
                    .collect(Collectors.toList()));
            allCols.add(new JdbcComponentMetadata.ValueCol(metadata.getColumns().getDynamicColumn().getValue()));
        }

        // select only given column names
        if (selectColNames.isPresent()) {
            return allCols.build().stream()
                    .map(c -> new SelectableJdbcColumnMetadata(c,
                            selectColNames.get().contains(c.getName()) || selectColNames.get().contains(c.getLabel())))
                    .collect(Collectors.toList());
        }
        // select all columns
        return allCols.build().stream()
                            .map(c -> new SelectableJdbcColumnMetadata(c, true))
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

}
