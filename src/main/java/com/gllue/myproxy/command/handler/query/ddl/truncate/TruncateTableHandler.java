package com.gllue.myproxy.command.handler.query.ddl.truncate;

import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.CommandHandlerException;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.util.SQLErrorUtils;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TruncateTableHandler extends AbstractDDLHandler {
  private static final String NAME = "Truncate table handler";

  public TruncateTableHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, sqlParser, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  protected Function<CommandResult, Promise<CommandResult>> truncateTablePromiseSupplier(
      QueryHandlerRequest request, List<String> queries) {
    var size = queries.size();
    var index = new AtomicInteger(0);
    return (result) -> {
      var i = index.getAndIncrement();
      if (i >= size) return null;

      var query = queries.get(i);
      return new Promise<CommandResult>(
              (callback) -> {
                submitQueryToBackendDatabase(request.getConnectionId(), query, callback);
              })
          .doCatch(
              (e) -> {
                if (SQLErrorUtils.isBadTable(e)) {
                  log.error(
                      "Failed to truncate table due to the table does not exist. [{}]", query);
                  return CommandResult.newEmptyResult();
                }
                throw new CommandHandlerException(e);
              });
    };
  }

  private void truncatePartitionTable(
      QueryHandlerRequest request,
      Callback<HandlerResult> callback,
      String database,
      PartitionTableMetaData table) {
    var queries =
        Arrays.stream(table.getTableNames())
            .map((x) -> String.format("`%s`.`%s`", database, x))
            .collect(Collectors.toList());

    Promise.chain(truncateTablePromiseSupplier(request, queries))
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

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLTruncateStatement) request.getStatement();
    if (stmt.getTableSources().size() != 1) {
      throw new BadSQLException("Truncate table statement has only one table source.");
    }

    ensureDatabaseExists(request);

    var datasource = request.getDatasource();
    var databaseName = request.getDatabase();
    var tableSource = stmt.getTableSources().get(0);
    var schema = SQLStatementUtils.getSchema(tableSource);
    if (schema == null) {
      schema = databaseName;
    }
    var tableName = SQLStatementUtils.getTableName(tableSource);
    var database = clusterState.getMetaData().getDatabase(datasource, schema);
    var table = database.getTable(tableName);
    if (table != null && table.getType() == TableType.PARTITION) {
      truncatePartitionTable(request, callback, schema, (PartitionTableMetaData) table);
      return;
    }

    submitQueryToBackendDatabase(
        request.getConnectionId(),
        request.getQuery(),
        WrappedHandlerResult.wrappedCallback(callback));
  }
}
