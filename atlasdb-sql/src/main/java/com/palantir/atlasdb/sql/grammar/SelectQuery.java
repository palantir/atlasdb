package com.palantir.atlasdb.sql.grammar;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLLexer;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;

@Value.Immutable
public abstract class SelectQuery {

    public static SelectQuery create(String sql) {
        AtlasSQLLexer lexer = new AtlasSQLLexer(new ANTLRInputStream(sql.toLowerCase()));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_queryContext query = parser.query().select_query();
        Preconditions.checkState(query != null, "Given sql does not parse as a select query: " + sql);

        ImmutableSelectQuery.Builder builder = ImmutableSelectQuery.builder();

        builder.table(query.table_reference().getText());
        builder.rangeRequest(RangeRequest.all());
        builder.columns(ColumnSelection.all());

        return builder.build();
    }

    @Value.Derived
    public TableRange tableRange() {
        return new TableRange(table(),
                rangeRequest().getStartInclusive(),
                rangeRequest().getEndExclusive(),
                columns().allColumnsSelected() ? Lists.newArrayList() : columns().getSelectedColumns(),
                batchSize());
    }

    public abstract String table();
    public abstract RangeRequest rangeRequest();
    public abstract ColumnSelection columns();

    @Value.Default
    public int batchSize() {
        return 2000;
    }

}
