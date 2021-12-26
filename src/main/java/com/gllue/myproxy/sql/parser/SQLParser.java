package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLParser {

  private final DruidSQLStatementParser druidParser;
  private final ExtendedSQLStatementParser extendedParser;
  private final CustomSQLStatementParser customParser;

  public SQLParser() {
    this(true);
  }

  public SQLParser(final boolean keepComments) {
    this.druidParser = new DruidSQLStatementParser(keepComments);
    this.extendedParser = new ExtendedSQLStatementParser();
    this.customParser = new CustomSQLStatementParser();
  }

  private SQLStatement tryToParseDruidUnsupportedStatement(String query) {
    return extendedParser.parseStatement(query);
  }

  private SQLStatement tryToParseCustomStatement(String query) {
    return customParser.parseStatement(query);
  }

  private SQLStatement parseStatement(String query) {
    return druidParser.parseStatement(query);
  }

  public SQLStatement parse(final String query) {
    if (log.isDebugEnabled()) {
      log.debug("Parse query: " + query);
    }
    var stmt = tryToParseDruidUnsupportedStatement(query);
    if (stmt != null) {
      return stmt;
    }
    stmt = tryToParseCustomStatement(query);
    if (stmt != null) {
      return stmt;
    }
    return parseStatement(query);
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
