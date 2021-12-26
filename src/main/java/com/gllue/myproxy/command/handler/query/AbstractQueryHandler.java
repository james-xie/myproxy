package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.CommandHandler;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.List;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractQueryHandler implements CommandHandler<QueryHandlerRequest> {
  protected final TransportService transportService;

  protected void submitQueryToBackendDatabase(
      int connectionId, String query, Callback<CommandResult> callback) {
    transportService.submitQueryToBackendDatabase(connectionId, query, callback);
  }

  protected void submitQueryAndDirectTransferResult(
      int connectionId, String query, Callback<HandlerResult> callback) {
    transportService.submitQueryAndDirectTransferResult(
        connectionId,
        query,
        QueryHandlerResult.wrappedCallbackWithDirectTransferredResult(callback));
  }

  protected Promise<CommandResult> submitQueryToBackendDatabase(int connectionId, String query) {
    return transportService.submitQueryToBackendDatabase(connectionId, query);
  }

  @Override
  public String toString() {
    return name();
  }
}
