package com.gllue.myproxy.command.handler.query.tcl;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.core.service.TransportService;

public class BeginStatementHandler extends AbstractQueryHandler {
  private static final String NAME = "Begin handler";

  public BeginStatementHandler(
      final TransportService transportService, final ThreadPool threadPool) {
    super(transportService, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    beginTransaction(request.getConnectionId())
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
