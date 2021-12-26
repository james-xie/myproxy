package com.gllue.myproxy.sql.parser;

import com.alibaba.druid.sql.ast.SQLStatement;

public interface SQLStatementParser {
  SQLStatement parseStatement(String query);
}
