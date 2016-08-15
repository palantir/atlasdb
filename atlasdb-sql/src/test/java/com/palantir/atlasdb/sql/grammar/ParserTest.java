package com.palantir.atlasdb.sql.grammar;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLLexer;
import com.palantir.atlasdb.sql.grammar.generated.AtlasSQLParser;

public class ParserTest {

    @Test
    public void testSelectClause() {
        AtlasSQLLexer lexer = new AtlasSQLLexer(
                new ANTLRInputStream("SELECT col1, col2 FROM keyspace.table WHERE col3 = 50 AND 4.5 > col4"));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_queryContext select = parser.select_query();
        Preconditions.checkState(select.column_clause().column_list().column_or_aggregate().get(0).column_name().getText().equals("col1"));
        Preconditions.checkState(select.column_clause().column_list().column_or_aggregate().get(1).column_name().getText().equals("col2"));
        Preconditions.checkState(select.table_reference().keyspace().getText().equals("keyspace"));
        Preconditions.checkState(select.table_reference().table_name().getText().equals("table"));
    }

}
