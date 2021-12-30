package com.gllue.myproxy.command.handler.query.dcl.kill;

import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement.Type;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.core.service.TransportService;

public class KillStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Kill handler";

  public KillStatementHandler(TransportService transportService) {
    super(transportService);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (MySqlKillStatement) request.getStatement();
    boolean killQuery = stmt.getType() == Type.QUERY;
    int threadId = ((SQLIntegerExpr) stmt.getThreadId()).getNumber().intValue();
    kill(request.getConnectionId(), threadId, killQuery)
        .then(
            (result) -> {
              callback.onSuccess(new WrappedHandlerResult(result));
              return true;
            },
            (e) -> {
              if (e instanceof IllegalArgumentException) {
                e = new NoSuchThreadException(threadId);
              }
              callback.onFailure(e);
              return false;
            });
  }
}
