package com.palantir.atlasdb.sql.grammar;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RuleContext;
import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLLexer;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;
import com.palantir.atlasdb.sql.jdbc.results.ParsedRowResult;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(String sql, AtlasDbService service) throws SQLException {
        AtlasSQLLexer lexer = new AtlasSQLLexer(new ANTLRInputStream(sql.toLowerCase()));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_queryContext query = parser.query().select_query();
        Preconditions.checkState(query != null, "Given sql does not parse as a select query: " + sql);

        String table = query.table_reference().getText();
        TableMetadata metadata = service.getTableMetadata(table);
        Preconditions.checkState(metadata != null, "Could not get table metadata for table " + table);

        List<JdbcColumnMetadata> allCols = makeAllColumns(metadata);
        List<JdbcColumnMetadata> selectedCols = makeSelectedColumns(query.column_clause().column_list(), allCols);
        Map<String, JdbcColumnMetadata> indexMap = makeLabelOrNameToMetadata(selectedCols);

        return ImmutableSelectQuery.builder()
                .table(table)
                .rangeRequest(RangeRequest.all())
                .columns(selectedCols)
                .postfilterPredicate(makePostfilterPredicate(query.where_clause()))
                .build();
    }

    private static List<JdbcColumnMetadata> makeAllColumns(TableMetadata metadata) {
        List<JdbcColumnMetadata> allCols = Lists.newArrayList();
        allCols.addAll(metadata.getRowMetadata().getRowParts().stream()
                               .map(JdbcColumnMetadata::create).collect(Collectors.toList()));
        if (metadata.getColumns().getNamedColumns() != null) {
            allCols.addAll(metadata.getColumns().getNamedColumns().stream()
                                   .map(JdbcColumnMetadata::create).collect(Collectors.toList()));
        }
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.add(JdbcColumnMetadata.create(metadata.getColumns().getDynamicColumn()));
        }
        return allCols;
    }

    private static List<JdbcColumnMetadata> makeSelectedColumns(@Nullable AtlasSQLParser.Column_listContext colListCtx,
                                                                List<JdbcColumnMetadata> allCols) {
        if (colListCtx == null) {
            return allCols;
        }
        List<String> requestedCols =
                colListCtx.column_name().stream().map(RuleContext::getText).collect(Collectors.toList());
        return allCols.stream().filter(c -> requestedCols.contains(c.getName())).collect(Collectors.toList());
    }

    public static Map<String, JdbcColumnMetadata> makeLabelOrNameToMetadata(List<JdbcColumnMetadata> selectedCols) {
        ImmutableMap.Builder<String, JdbcColumnMetadata> builder = ImmutableMap.builder();
        selectedCols.stream().collect(Collectors.toMap(JdbcColumnMetadata::getName, Function.identity()));
        selectedCols.stream()
                .filter(m -> !m.getLabel().equals(m.getName()))
                .collect(Collectors.toMap(JdbcColumnMetadata::getLabel, Function.identity()));
        return builder.build();
    }

    private static Predicate<ParsedRowResult> makePostfilterPredicate(@Nullable AtlasSQLParser.Where_clauseContext whereCtx) {
        if (whereCtx == null) {
            return parsedRowResult -> true;
        }
        return (Predicate<ParsedRowResult>) new WhereClauseVisitor().visit(whereCtx);
    }

    @Value.Derived
    public TableRange tableRange() {
        List<byte[]> cols = columns()
                .stream()
                .filter(JdbcColumnMetadata::isCol)
                .map(c -> c.getName().getBytes())
                .collect(Collectors.toList());
        return new TableRange(table(),
                rangeRequest().getStartInclusive(),
                rangeRequest().getEndExclusive(),
                cols,
                batchSize());
    }

    public abstract String table();
    public abstract RangeRequest rangeRequest();
    public abstract List<JdbcColumnMetadata> columns();
    public abstract Map<String, JdbcColumnMetadata> labelOrNameToMetadata();
    public abstract Predicate<ParsedRowResult> postfilterPredicate();

    @Value.Default
    public int batchSize() {
        return 2000;
    }

}
