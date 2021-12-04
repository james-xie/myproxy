package com.gllue.command.handler.query;

import static org.mockito.Mockito.when;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.AssertUtils;
import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigPropertyKey;
import com.gllue.sql.parser.SQLParser;
import org.mockito.Mock;

public abstract class BaseQueryHandlerTest {
  @Mock protected Configurations configurations;

  protected final SQLParser sqlParser = new SQLParser();

  protected void mockConfigurations() {
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE))
        .thenReturn(100);
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK))
        .thenReturn(0.9);
  }

  protected MySqlCreateTableStatement parseCreateTableQuery(final String query) {
    return (MySqlCreateTableStatement) sqlParser.parse(query);
  }

  protected SQLAlterTableStatement parseAlterTableQuery(final String query) {
    return (SQLAlterTableStatement) sqlParser.parse(query);
  }

  protected void assertSQLEquals(final SQLStatement expected, final SQLStatement actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }

  protected void assertSQLEquals(final String expected, final SQLStatement actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }

  protected void assertSQLEquals(final String expected, final String actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }
}
