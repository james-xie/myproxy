package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.CommandHandler;
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
   * Check whether the database metadata has been created. if not exists, create a new database metadata.
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

  @Override
  public String toString() {
    return name();
  }
}
