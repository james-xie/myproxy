package com.gllue.myproxy.command.handler.query.dml.select;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public class CheckTableSourceVisitor extends MySqlASTVisitorAdapter {
  private boolean hasTableSource = false;

  @Override
  public boolean visit(SQLExprTableSource x) {
    hasTableSource = true;
    return false;
  }

  public boolean visit(SQLSelect x) {
    return !hasTableSource;
  }

  public boolean hasTableSource() {
    return hasTableSource;
  }
}
