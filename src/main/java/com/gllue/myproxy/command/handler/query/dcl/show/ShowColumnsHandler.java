package com.gllue.myproxy.command.handler.query.dcl.show;

import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.toSQLString;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.MergeQueryResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.command.result.query.SimpleQueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ShowColumnsHandler extends AbstractQueryHandler {
  private static final String NAME = "Show columns handler";
  private static final int FIELD_INDEX = 0;
  private static final int TYPE_INDEX = 1;

  private final ClusterState clusterState;

  public ShowColumnsHandler(
      final TransportService transportService,
      final ThreadPool threadPool,
      final ClusterState clusterState) {
    super(transportService, threadPool);
    this.clusterState = clusterState;
  }

  @Override
  public String name() {
    return NAME;
  }

  private Promise<QueryResult> showPartitionTableColumns(
      QueryHandlerRequest request, PartitionTableMetaData table) {
    var stmt = (SQLShowColumnsStatement) request.getStatement();
    var tableNames = table.getTableNames();
    var size = tableNames.length;
    var index = new AtomicInteger(0);
    var connectionId = request.getConnectionId();
    var promise =
        Promise.all(
            () -> {
              var i = index.getAndIncrement();
              if (i >= size) return null;
              var newStmt = (SQLShowColumnsStatement) stmt.clone();
              newStmt.setTable(new SQLIdentifierExpr(quoteName(tableNames[i])));
              var query = toSQLString(newStmt);
              return new Promise<CommandResult>(
                  (callback) -> submitQueryToBackendDatabase(connectionId, query, callback));
            });

    return promise.then(
        (result) -> {
          var queryResults =
              result.stream().map(CommandResult::getQueryResult).collect(Collectors.toList());
          return new MergeQueryResult(queryResults);
        });
  }

  private String convertColumnType(ColumnMetaData column, String columnType) {
    if (column.getType() == ColumnType.ENCRYPT) {
      columnType = columnType.toLowerCase().replace("varbinary", "encrypt");
    }
    return columnType;
  }

  private Promise<QueryResult> showColumns(QueryHandlerRequest request, TableMetaData table) {
    var stmt = (SQLShowColumnsStatement) request.getStatement();
    if (stmt.getWhere() != null) {
      throw new BadSQLException(
          String.format(
              "Show columns with where clause is not supported for table [%s].", table.getName()));
    }

    Promise<QueryResult> promise;
    if (table.getType() == TableType.PARTITION) {
      promise = showPartitionTableColumns(request, (PartitionTableMetaData) table);
    } else {
      promise =
          submitQueryToBackendDatabase(request.getConnectionId(), request.getQuery())
              .then(CommandResult::getQueryResult);
    }
    return promise.then(
        (result) -> {
          var metaData = result.getMetaData();
          assert "Field".equalsIgnoreCase(metaData.getColumnLabel(FIELD_INDEX));
          assert "Type".equalsIgnoreCase(metaData.getColumnLabel(TYPE_INDEX));

          var rows = new ArrayList<String[]>();
          while (result.next()) {
            var columnName = result.getStringValue(FIELD_INDEX);
            var column = table.getColumn(columnName);
            if (column == null) {
              continue;
            }
            var row = new String[metaData.getColumnCount()];
            for (int i = 0; i < row.length; i++) {
              row[i] = result.getStringValue(i);
            }
            row[TYPE_INDEX] = convertColumnType(column, row[TYPE_INDEX]);
            rows.add(row);
          }
          return new SimpleQueryResult(result.getMetaData(), rows);
        });
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (SQLShowColumnsStatement) request.getStatement();
    String databaseName = request.getDatabase();
    var tableName = unquoteName(stmt.getTable().getSimpleName());
    if (stmt.getDatabase() != null) {
      databaseName = unquoteName(stmt.getDatabase().getSimpleName());
    }

    var database = clusterState.getMetaData().getDatabase(request.getDatasource(), databaseName);
    if (database == null || !database.hasTable(tableName)) {
      submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
      return;
    }

    showColumns(request, database.getTable(tableName))
        .then(
            (result) -> {
              callback.onSuccess(new QueryHandlerResult(result));
              return true;
            },
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
