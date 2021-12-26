package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DruidSQLStatementParser implements SQLStatementParser {
  private static final DbType DB_TYPE = JdbcConstants.MYSQL;

  private final boolean keepComments;

  public DruidSQLStatementParser() {
    this(true);
  }

  public DruidSQLStatementParser(final boolean keepComments) {
    this.keepComments = keepComments;
  }

  @Override
  public SQLStatement parseStatement(String query) {
    try {
      return SQLUtils.parseSingleStatement(query, DB_TYPE, keepComments);
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Failed to parse sql. [sql:{}]", query, e);
      }
      throw new SQLParseException(e.getMessage());
    }
  }
}
