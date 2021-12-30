package com.gllue.myproxy.command.handler.query.dcl.show;

import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.result.query.FilteredQueryResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.HashSet;
import java.util.function.Predicate;

public class ShowTablesHandler extends AbstractQueryHandler {
  public static final String NAME = "Show tables handler";

  private final ClusterState clusterState;

  public ShowTablesHandler(
      final TransportService transportService,
      final ClusterState clusterState,
      final ThreadPool threadPool) {
    super(transportService, threadPool);
    this.clusterState = clusterState;
  }

  @Override
  public String name() {
    return NAME;
  }

  private void filterTablesHandler(
      QueryHandlerRequest request, Callback<HandlerResult> callback, DatabaseMetaData database) {
    var extensionTables = new HashSet<String>();
    for (var table : database.getTables()) {
      if (table.getType() == TableType.PARTITION) {
        var partitionTable = (PartitionTableMetaData) table;
        for (var extTable : partitionTable.getExtensionTables()) {
          extensionTables.add(extTable.getName());
        }
      }
    }

    Predicate<QueryResult> extensionTablesFilter =
        (queryResult) -> {
          var tableName = queryResult.getStringValue(0);
          return !extensionTables.contains(tableName);
        };

    submitQueryToBackendDatabase(request.getConnectionId(), request.getQuery())
        .then(
            (result) -> {
              callback.onSuccess(
                  new QueryHandlerResult(
                      result.getWarnings(),
                      new FilteredQueryResult(result.getQueryResult(), extensionTablesFilter)));
              return true;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    if (request.getDatabase() == null) {
      throw new NoDatabaseException();
    }

    var stmt = (SQLShowTablesStatement) request.getStatement();
    String databaseName;
    if (stmt.getDatabase() != null) {
      databaseName = unquoteName(stmt.getDatabase().getSimpleName());
    } else {
      databaseName = request.getDatabase();
    }

    var database = clusterState.getMetaData().getDatabase(request.getDatasource(), databaseName);
    if (database != null) {
      filterTablesHandler(request, callback, database);
    } else {
      submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
    }
  }
}
