package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import java.util.regex.Pattern;

public class ExtendedSQLStatementParser implements SQLStatementParser {
  private static final Pattern BEGIN_PATTERN =
      Pattern.compile("^\\s*begin\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);


  private boolean isBeginQuery(String query) {
    return BEGIN_PATTERN.matcher(query).matches();
  }

  @Override
  public SQLStatement parseStatement(String query) {
    if (isBeginQuery(query)) {

    }
    return null;
  }
}
