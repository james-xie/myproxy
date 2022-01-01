package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.gllue.myproxy.sql.stmt.SQLShowMetricsStatement;
import java.util.regex.Pattern;

public class CustomSQLStatementParser implements SQLStatementParser {
  private static final Pattern SHOW_METRICS_PATTERN =
      Pattern.compile("^\\s*show\\s*metrics\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);

  private boolean isShowMetricsQuery(String query) {
    return SHOW_METRICS_PATTERN.matcher(query).matches();
  }

  @Override
  public SQLStatement parseStatement(String query) {
    if (isShowMetricsQuery(query)) {
      return new SQLShowMetricsStatement();
    }

    return null;
  }
}
