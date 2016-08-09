package com.palantir.atlasdb.sql.grammar;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class ParserTest {

    @Test
    public void testSelectClause() {
        AtlasSQLLexer lexer = new AtlasSQLLexer(new ANTLRInputStream("select col1, col2 from table"));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_clauseContext select = parser.select_clause();
        Preconditions.checkState(select.column_clause().column_list().column_name(0).getText().equals("col1"));
        Preconditions.checkState(select.column_clause().column_list().column_name(1).getText().equals("col2"));
        Preconditions.checkState(select.table_name().ID().getText().equals("table"));
    }

}
