package com.gllue;

import static org.junit.Assert.assertEquals;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.gllue.common.util.SQLStatementUtils;
import com.gllue.sql.parser.SQLParser;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AssertUtils {
  public static void assertSQLEquals(final SQLStatement expected, final SQLStatement actual) {
    assertEquals(expected.getClass(), actual.getClass());
    var expectedSql = SQLStatementUtils.toSQLString(expected);
    var actualSql = SQLStatementUtils.toSQLString(actual);
    if (expectedSql.endsWith(";")) {
      expectedSql = expectedSql.substring(0, expectedSql.length() - 1);
    }
    if (actualSql.endsWith(";")) {
      actualSql = actualSql.substring(0, actualSql.length() - 1);
    }
    assertEquals(expectedSql, actualSql);
  }

  public static void assertSQLEquals(final String expected, final SQLStatement actual) {
    var actualSql = SQLStatementUtils.toSQLString(actual);
    assertSQLEquals(expected, actualSql);
  }

  public static void assertSQLEquals(final String expected, final String actual) {
    final SQLParser sqlParser = new SQLParser();
    var expectedStmt = sqlParser.parse(expected);
    var actualStmt = sqlParser.parse(actual);
    assertSQLEquals(expectedStmt, actualStmt);
  }
}
