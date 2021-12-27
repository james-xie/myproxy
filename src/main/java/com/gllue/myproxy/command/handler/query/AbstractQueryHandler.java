package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.command.handler.CommandHandler;
import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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

  protected Promise<Boolean> beginTransaction(int connectionId) {
    return transportService.beginTransaction(connectionId);
  }

  protected Promise<Boolean> commitTransaction(int connectionId) {
    return transportService.commitTransaction(connectionId);
  }

  protected Promise<Boolean> rollbackTransaction(int connectionId) {
    return transportService.rollbackTransaction(connectionId);
  }

  protected Promise<List<CommandResult>> executeQueries(
      QueryHandlerRequest request, List<String> queries) {
    var size = queries.size();
    var index = new AtomicInteger(0);
    var connectionId = request.getConnectionId();
    return Promise.all(
        () -> {
          var i = index.getAndIncrement();
          if (i >= size) return null;
          var query = queries.get(i);
          return new Promise<>(
              (callback) -> {
                submitQueryToBackendDatabase(connectionId, query, callback);
              });
        });
  }

  protected <R, T> Function<R, T> throwWrappedException(Throwable e) {
    return (v) -> {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }

      throw new CommandHandlerException(e);
    };
  }

  protected Promise<List<CommandResult>> executeQueriesAtomically(
      QueryHandlerRequest request, List<String> queries) {
    var sessionContext = request.getSessionContext();
    if (sessionContext.isTransactionOpened()) {
      return executeQueries(request, queries);
    }

    var connectionId = request.getConnectionId();
    return beginTransaction(connectionId)
        .thenAsync((v) -> executeQueries(request, queries))
        .thenAsync(
            (result) -> commitTransaction(connectionId).then((v) -> result),
            (e) -> rollbackTransaction(connectionId).then(throwWrappedException(e)));
  }

  @Override
  public String toString() {
    return name();
  }
}