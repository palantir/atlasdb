package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.antlr.v4.runtime.RuleContext;
import org.immutables.value.Value;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.JdbcComponentMetadata;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(TableMetadata metadata,
                                     AtlasSQLParser.Select_queryContext query) throws SQLException {
        List<JdbcColumnMetadata> allCols = makeAllColumns(metadata);
        List<JdbcColumnMetadata> selectedCols = makeSelectedColumns(query.column_clause().column_list(), allCols);
        WhereClause where = WhereClause.create(query.where_clause(), allCols);
        return ImmutableSelectQuery.builder()
                .table(getTableName(query))
                .rangeRequest(RangeRequest.all())
                .allColumns(allCols)
                .selectedColumns(selectedCols)
                .prefilterConstraint(where.prefilterConstraints())
                .postfilterPredicate(where.postfilterPredicate())
                .build();
    }

    public static String getTableName(AtlasSQLParser.Select_queryContext query) {
        return query.table_reference().getText();
    }

    private static List<JdbcColumnMetadata> makeAllColumns(TableMetadata metadata) {
        List<JdbcColumnMetadata> allCols = Lists.newArrayList();
        allCols.addAll(metadata.getRowMetadata().getRowParts().stream()
                               .map(JdbcComponentMetadata.RowComp::new).collect(Collectors.toList()));
        if (metadata.getColumns().getNamedColumns() != null) {
            allCols.addAll(metadata.getColumns().getNamedColumns().stream()
                                   .map(JdbcComponentMetadata.NamedCol::new).collect(Collectors.toList()));
        }
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.addAll(metadata.getColumns().getDynamicColumn().getColumnNameDesc().getRowParts()
                                   .stream().map(JdbcComponentMetadata.ColComp::new).collect(Collectors.toList()));
            allCols.add(new JdbcComponentMetadata.DynCol(metadata.getColumns().getDynamicColumn()));
        }
        return allCols;
    }

    private static List<JdbcColumnMetadata> makeSelectedColumns(@Nullable AtlasSQLParser.Column_listContext colListCtx,
                                                                List<JdbcColumnMetadata> allCols) {
        if (colListCtx == null) {
            if (JdbcColumnMetadata.anyDynamicColumns(allCols)) {
                // when dynamic columns are requested, the retainColumns clause must be empty. Return only rowComps!
                return allCols.stream().filter(JdbcColumnMetadata::isRowComp).collect(Collectors.toList());
            } else {
                return allCols;
            }
        }
        List<String> requestedCols =
                colListCtx.column_name().stream().map(RuleContext::getText).collect(Collectors.toList());
        return allCols.stream().filter(c -> requestedCols.contains(c.getName())).collect(Collectors.toList());
    }

    @Value.Derived
    public TableRange tableRange() {
        List<byte[]> cols = selectedColumns()
                .stream()
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
    public abstract List<JdbcColumnMetadata> allColumns();
    public abstract List<JdbcColumnMetadata> selectedColumns();
    public abstract Map<String, JdbcColumnMetadata> labelOrNameToMetadata();
    public abstract RowComponentConstraint prefilterConstraint();
    public abstract Predicate<ParsedRowResult> postfilterPredicate();

    @Value.Default
    public int batchSize() {
        return 2000;
    }

}
