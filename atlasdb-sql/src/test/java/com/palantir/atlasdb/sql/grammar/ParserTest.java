package com.palantir.atlasdb.sql.grammar;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class ParserTest {

    @Test
    public void testSelectClause() {
        AtlasSQLLexer lexer = new AtlasSQLLexer(new ANTLRInputStream("SELECT col1, col2 FROM keyspace.table WHERE col3 = 50 AND col4 BETWEEN 10 AND 20".toLowerCase()));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        AtlasSQLParser parser = new AtlasSQLParser(tokens);
        AtlasSQLParser.Select_clauseContext select = parser.select_clause();
        Preconditions.checkState(select.column_clause().column_list().column_name(0).getText().equals("col1"));
        Preconditions.checkState(select.column_clause().column_list().column_name(1).getText().equals("col2"));
        Preconditions.checkState(select.table_reference().keyspace().getText().equals("keyspace"));
        Preconditions.checkState(select.table_reference().table_name().getText().equals("table"));
        Preconditions.checkState(select.where_clause().expression().simple_expression(0).left_element().element().getText().equals("col3"));
        Preconditions.checkState(select.where_clause().expression().simple_expression(0).relational_op().getText().equals("="));
        Preconditions.checkState(select.where_clause().expression().simple_expression(0).right_element().element().getText().equals("50"));
        Preconditions.checkState(select.where_clause().expression().simple_expression(1).target_element().element().getText().equals("col4"));
        Preconditions.checkState(select.where_clause().expression().simple_expression(1).left_element().element().getText().equals("10"));
        Preconditions.checkState(select.where_clause().expression().simple_expression(1).right_element().element().getText().equals("20"));
    }

}
