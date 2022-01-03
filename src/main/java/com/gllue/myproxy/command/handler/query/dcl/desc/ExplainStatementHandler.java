package com.gllue.myproxy.command.handler.query.dcl.desc;

import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.dcl.show.ShowColumnsHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.core.service.TransportService;

public class ExplainStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Explain handler";

  private final ShowColumnsHandler showColumnsHandler;

  public ExplainStatementHandler(
      final TransportService transportService,
      final ThreadPool threadPool,
      final ShowColumnsHandler showColumnsHandler) {
    super(transportService, threadPool);
    this.showColumnsHandler = showColumnsHandler;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (MySqlExplainStatement) request.getStatement();
    if (stmt.getTableName() != null) {
      var showColumnsStmt = new SQLShowColumnsStatement();
      showColumnsStmt.setTable(stmt.getTableName());
      showColumnsHandler.showTableColumns(
          request.getConnectionId(),
          request.getDatasource(),
          request.getDatabase(),
          showColumnsStmt,
          callback);
      return;
    }

    submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
  }
}
