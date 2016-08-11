package com.palantir.atlasdb.sql.grammar;

import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLLexer;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;
import com.palantir.atlasdb.sql.jdbc.results.JdbcColumnMetadata;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(String sql, AtlasDbService service) {
        AtlasSQLLexer lexer = new AtlasSQLLexer(new ANTLRInputStream(sql.toLowerCase()));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_queryContext query = parser.query().select_query();
        Preconditions.checkState(query != null, "Given sql does not parse as a select query: " + sql);

        String table = query.table_reference().getText();
        TableMetadata metadata = service.getTableMetadata(table);

        ImmutableSelectQuery.Builder builder = ImmutableSelectQuery.builder();
        builder.table(table);
        builder.rangeRequest(RangeRequest.all());

        List<JdbcColumnMetadata> allCols = getAllColumns(metadata);
        if (query.column_clause().column_list() != null) {
            List<String> requestedCols =
                    query.column_clause().column_list().column_name().stream().map(c -> c.getText()).collect(Collectors.toList());
            builder.columns(allCols.stream().filter(c -> requestedCols.contains(c.getName())).collect(Collectors.toList()));
        } else {
            builder.columns(allCols);
        }

        return builder.build();
    }

    private static List<JdbcColumnMetadata> getAllColumns(TableMetadata metadata) {
        List<JdbcColumnMetadata> allCols = Lists.newArrayList();
        allCols.addAll(metadata.getRowMetadata().getRowParts().stream()
                .map(JdbcColumnMetadata::create).collect(Collectors.toList()));
        allCols.addAll(metadata.getColumns().getNamedColumns().stream()
                .map(JdbcColumnMetadata::create).collect(Collectors.toList()));
        if (metadata.getColumns().getDynamicColumn() != null) {
            allCols.add(JdbcColumnMetadata.create(metadata.getColumns().getDynamicColumn()));
        }
        return allCols;
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

    @Value.Default
    public int batchSize() {
        return 2000;
    }

}
