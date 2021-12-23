package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.CommandHandler;
import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.CreateDatabaseCommand;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.command.context.MultiDatabasesCommandContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractQueryHandler<Result extends HandlerResult>
    implements CommandHandler<QueryHandlerRequest, Result> {
  private final PersistRepository repository;
  protected final Configurations configurations;
  protected final ClusterState clusterState;
  protected final TransportService transportService;
  protected final SQLParser sqlParser;

  protected AbstractQueryHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser) {
    this.repository = repository;
    this.configurations = configurations;
    this.clusterState = clusterState;
    this.transportService = transportService;
    this.sqlParser = sqlParser;
  }

  protected CommandExecutionContext<MultiDatabasesMetaData> newCommandExecutionContext() {
    return new MultiDatabasesCommandContext(clusterState.getMetaData(), repository, configurations);
  }

  protected enum LockType {
    READ,
    WRITE
  }

  protected <T> Promise<T> lockTables(
      final int connectionId,
      final Function<CommandResult, Promise<T>> operation,
      final LockType lockType,
      final String... tableNames) {
    Consumer<Callback<CommandResult>> lockTable =
        (cb) -> {
          var queryBuilder = new StringBuilder();
          queryBuilder.append("LOCK TABLES");
          for (var tableName : tableNames) {
            queryBuilder.append(" `");
            queryBuilder.append(tableName);
            queryBuilder.append("` ");
            queryBuilder.append(lockType.name());
          }
          submitQueryToBackendDatabase(connectionId, queryBuilder.toString(), cb);
        };

    BiFunction<T, Throwable, Promise<T>> unlockTables =
        (value, exception) -> {
          return new Promise<T>(
              (cb) -> {
                var query = "UNLOCK TABLES";
                var callback =
                    new Callback<CommandResult>() {
                      @Override
                      public void onSuccess(CommandResult result) {
                        cb.onSuccess(value);
                      }

                      @Override
                      public void onFailure(Throwable e) {
                        cb.onFailure(exception);
                      }
                    };
                submitQueryToBackendDatabase(connectionId, query, callback);
              });
        };

    return new Promise<CommandResult>(lockTable)
        .thenAsync((v) -> operation.apply(v).doFinallyAsync(unlockTables));
  }

  /**
   * Check whether the database metadata has been created. if not exists, create a new database
   * metadata.
   *
   * @param request handler request
   * @return true if the database already exists.
   */
  protected boolean ensureDatabaseExists(QueryHandlerRequest request) {
    var databaseName = request.getDatabase();
    if (databaseName != null) {
      var datasource = request.getDatasource();
      var database = clusterState.getMetaData().getDatabase(datasource, databaseName);
      if (database == null) {
        var command = new CreateDatabaseCommand(datasource, databaseName);
        command.execute(newCommandExecutionContext());
        return false;
      }
    } else {
      throw new NoDatabaseException();
    }
    return true;
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

  protected void submitQueryToBackendDatabase(
      int connectionId, String query, Callback<CommandResult> callback) {
    transportService.submitQueryToBackendDatabase(connectionId, query, callback);
  }

  protected void submitQueryAndDirectTransferResult(
      int connectionId, String query, Callback<CommandResult> callback) {
    transportService.submitQueryAndDirectTransferResult(connectionId, query, callback);
  }

  protected Promise<CommandResult> submitQueryToBackendDatabase(int connectionId, String query) {
    return transportService.submitQueryToBackendDatabase(connectionId, query);
  }

  private Promise<CommandResult> executeQueries(QueryHandlerRequest request, List<String> queries) {
    var size = queries.size();
    var index = new AtomicInteger(0);
    var connectionId = request.getConnectionId();
    return Promise.chain(
        (result) -> {
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

  protected Promise<CommandResult> executeQueriesAtomically(
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
