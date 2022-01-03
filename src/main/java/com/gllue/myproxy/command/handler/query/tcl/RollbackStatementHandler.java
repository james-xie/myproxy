package com.gllue.myproxy.command.handler.query.tcl;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.core.service.TransportService;

public class RollbackStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Rollback handler";

  public RollbackStatementHandler(
      final TransportService transportService, final ThreadPool threadPool) {
    super(transportService, threadPool);
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    rollbackTransaction(request.getConnectionId())
        .then(
            (result) -> {
              callback.onSuccess(new WrappedHandlerResult(result));
              return true;
            },
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
