package com.gllue.command.handler.query.ddl;

import static com.gllue.common.util.SQLStatementUtils.columnDefaultExpr;
import static com.gllue.common.util.SQLStatementUtils.columnIsNullable;
import static com.gllue.common.util.SQLStatementUtils.toSQLString;
import static com.gllue.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.cluster.ClusterState;
import com.gllue.command.handler.CommandHandlerException;
import com.gllue.command.handler.query.AbstractQueryHandler;
import com.gllue.command.handler.query.DefaultHandlerResult;
import com.gllue.command.handler.query.QueryHandlerRequest;
import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.common.Promise;
import com.gllue.common.util.SQLErrorUtils;
import com.gllue.config.Configurations;
import com.gllue.metadata.command.AbstractTableUpdateCommand;
import com.gllue.metadata.command.CreatePartitionTableCommand;
import com.gllue.metadata.command.CreateTableCommand;
import com.gllue.metadata.command.MetaDataCommand;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.TableType;
import com.gllue.repository.PersistRepository;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.core.service.TransportService;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractDDLHandler extends AbstractQueryHandler<DefaultHandlerResult> {
  protected AbstractDDLHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  protected AbstractTableUpdateCommand.Column buildCommandColumn(SQLColumnDefinition columnDef) {
    return new AbstractTableUpdateCommand.Column(
        unquoteName(columnDef.getColumnName()),
        ColumnType.getColumnType(columnDef.getDataType().getName()),
        columnIsNullable(columnDef),
        columnDefaultExpr(columnDef));
  }

  protected MetaDataCommand<MultiDatabasesMetaData> buildCreateStandardTableCommand(
      String datasource, String database, String table, MySqlCreateTableStatement stmt) {
    var columnDefs = stmt.getColumnDefinitions();
    var columns = new CreateTableCommand.Column[columnDefs.size()];
    for (int i = 0; i < columns.length; i++) {
      var columnDef = columnDefs.get(i);
      columns[i] = buildCommandColumn(columnDef);
    }
    return new CreateTableCommand(datasource, database, table, TableType.STANDARD, columns);
  }

  protected MetaDataCommand<MultiDatabasesMetaData> buildCreatePartitionTableCommand(
      String datasource, String database, String table, List<MySqlCreateTableStatement> stmtList) {
    var tables = new CreatePartitionTableCommand.Table[stmtList.size()];
    for (int s = 0; s < stmtList.size(); s++) {
      var stmt = stmtList.get(s);
      var tableName = unquoteName(stmt.getTableName());

      var columnDefs = stmt.getColumnDefinitions();
      var columns = new CreatePartitionTableCommand.Column[columnDefs.size()];
      for (int i = 0; i < columns.length; i++) {
        var columnDef = columnDefs.get(i);
        columns[i] = buildCommandColumn(columnDef);
      }

      tables[s] = new CreatePartitionTableCommand.Table(tableName, columns);
    }

    return new CreatePartitionTableCommand(
        datasource, database, table, tables[0], Arrays.copyOfRange(tables, 1, tables.length));
  }

  protected MetaDataCommand<MultiDatabasesMetaData> buildCreateTableCommand(
      String datasource,
      String database,
      String table,
      List<MySqlCreateTableStatement> stmtList,
      boolean isPartitionTable) {
    if (isPartitionTable) {
      return buildCreateStandardTableCommand(datasource, database, table, stmtList.get(0));
    } else {
      return buildCreatePartitionTableCommand(datasource, database, table, stmtList);
    }
  }

  protected void submitQueryToBackendDatabase(
      QueryHandlerRequest request, String query, Callback<DefaultHandlerResult> callback) {
    transportService.submitQueryToBackendDatabase(
        request.getBackendConnectionId(),
        query,
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            callback.onSuccess(DefaultHandlerResult.getInstance());
          }

          @Override
          public void onFailure(Throwable e) {
            callback.onFailure(e);
          }
        });
  }

  protected Promise<String> showCreateTable(QueryHandlerRequest request, String tableName) {
    return new Promise<>(
        (cb) -> {
          var connectionId = request.getBackendConnectionId();
          var query = String.format("SHOW CREATE TABLE `%s`", tableName);
          Callback<CommandResult> callback =
              new Callback<CommandResult>() {
                @Override
                public void onSuccess(CommandResult result) {
                  var queryResult = result.getQueryResult();
                  cb.onSuccess(queryResult.getStringValue(1));
                }

                @Override
                public void onFailure(Throwable e) {
                  cb.onFailure(e);
                }
              };
          transportService.submitQueryToBackendDatabase(connectionId, query, callback);
        });
  }

  protected Promise<Map<String, SQLColumnDefinition>> showCreateTableReturnColumnDefMap(
      QueryHandlerRequest request, String tableName) {
    return showCreateTable(request, tableName)
        .then(
            (createTableQuery) -> {
              var createTableStmt = (MySqlCreateTableStatement) sqlParser.parse(createTableQuery);
              return createTableStmt.getColumnDefinitions().stream()
                  .collect(Collectors.toMap(x -> unquoteName(x.getColumnName()), x -> x));
            });
  }

  protected Function<CommandResult, Promise<CommandResult>> createTablePromiseSupplier(
      QueryHandlerRequest request, List<MySqlCreateTableStatement> statements) {
    var index = new AtomicInteger(statements.size());
    return (result) -> {
      var i = index.decrementAndGet();
      if (i < 0) return null;

      var stmt = statements.get(i);
      var query = toSQLString(stmt);
      return new Promise<CommandResult>(
              (callback) -> {
                transportService.submitQueryToBackendDatabase(
                    request.getBackendConnectionId(), query, callback);
              })
          .doCatchAsync(
              (e) -> {
                if (SQLErrorUtils.isTableAlreadyExists(e)) {
                  var tableName = unquoteName(stmt.getTableName());
                  return dropTableThenRetry(request, tableName, query);
                } else {
                  log.error("Failed to execute create table query. [query={}]", query);
                  throw new CommandHandlerException(e);
                }
              });
    };
  }

  protected Promise<CommandResult> dropTable(QueryHandlerRequest request, final String tableName) {
    var query = String.format("DROP TABLE IF EXISTS `%s`", tableName);
    return transportService.submitQueryToBackendDatabase(request.getBackendConnectionId(), query);
  }

  protected Promise<CommandResult> dropTableThenRetry(
      QueryHandlerRequest request, String tableName, String queryForRetry) {
    return dropTable(request, tableName)
        .thenAsync(
            (v) -> {
              return transportService.submitQueryToBackendDatabase(
                  request.getBackendConnectionId(), queryForRetry);
            },
            (e) -> {
              log.error("Failed to execute drop table query. [table={}]", tableName);
              throw new CommandHandlerException(e);
            });
  }
}
