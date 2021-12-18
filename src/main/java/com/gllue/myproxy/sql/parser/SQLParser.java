package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLParser {
  private static final DbType DB_TYPE = JdbcConstants.MYSQL;

  private final boolean keepComments;

  public SQLParser() {
    this(true);
  }

  public SQLParser(final boolean keepComments) {
    this.keepComments = keepComments;
  }

  public SQLStatement parse(final String sql) {
    try {
      return SQLUtils.parseSingleStatement(sql, DB_TYPE, keepComments);
    } catch (Exception e) {
      if (log.isTraceEnabled()) {
        log.trace("Failed to parse sql. [sql:{}]", sql, e);
      }
      throw new SQLParseException(e.getMessage());
    }
  }

  /**
   * # { EXTENSION_COLUMNS: ["column1", "column2", ...] }
   *
   * @param comments
   * @return
   */
  public Map<SQLCommentAttributeKey, Object> parseComments(List<String> comments) {
    if (comments == null || comments.isEmpty()) {
      return Map.of();
    }
    // todo: parse comments
    return null;
  }
}
