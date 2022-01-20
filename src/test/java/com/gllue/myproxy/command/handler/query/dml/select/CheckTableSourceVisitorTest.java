package com.gllue.myproxy.command.handler.query.dml.select;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.gllue.myproxy.sql.parser.SQLParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CheckTableSourceVisitorTest {
  private final SQLParser sqlParser = new SQLParser();

  SQLSelectStatement parseSelectQuery(final String query) {
    return (SQLSelectStatement) sqlParser.parse(query);
  }

  @Test
  public void testHasTableSource() {
    var visitor = new CheckTableSourceVisitor();
    var stmt = parseSelectQuery("select sleep(1)");
    stmt.accept(visitor);
    assertFalse(visitor.hasTableSource());

    stmt = parseSelectQuery("select version()");
    stmt.accept(visitor);
    assertFalse(visitor.hasTableSource());

    stmt = parseSelectQuery("select * from t");
    stmt.accept(visitor);
    assertTrue(visitor.hasTableSource());

    stmt = parseSelectQuery("select * from (select * from table1) t");
    stmt.accept(visitor);
    assertTrue(visitor.hasTableSource());

    stmt = parseSelectQuery("select 1 in (select id from table1)");
    stmt.accept(visitor);
    assertTrue(visitor.hasTableSource());
  }
}
