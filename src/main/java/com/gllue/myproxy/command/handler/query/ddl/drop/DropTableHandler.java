package com.gllue.myproxy.command.handler.query.ddl.drop;

import static com.gllue.myproxy.common.util.SQLStatementUtils.getSchema;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getTableName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;

import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.util.SQLErrorUtils;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.DropTableCommand;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;

public class DropTableHandler extends AbstractDDLHandler {
  private static final String NAME = "Drop table handler";

  public DropTableHandler(
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

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    ensureDatabaseExists(request);

    var datasource = request.getDatasource();
    var databaseName = request.getDatabase();
    var stmt = (SQLDropTableStatement) request.getStatement();

    var dropTableCommands = new ArrayList<DropTableCommand>();
    var extensionTableSources = new ArrayList<SQLExprTableSource>();
    for (var tableSource : stmt.getTableSources()) {
      var schema = getSchema(tableSource);
      var tableName = getTableName(tableSource);
      if (schema == null) {
        schema = databaseName;
      }

      var database = clusterState.getMetaData().getDatabase(datasource, schema);
      var table = database.getTable(tableName);
      if (table != null) {
        if (table.getType() == TableType.PARTITION) {
          var partitionTable = (PartitionTableMetaData) table;
          for (var extTable : partitionTable.getExtensionTables()) {
            extensionTableSources.add(
                new SQLExprTableSource(
                    new SQLPropertyExpr(quoteName(schema), quoteName(extTable.getName()))));
          }
        }
        dropTableCommands.add(new DropTableCommand(datasource, schema, tableName));
      }
    }

    var query = request.getQuery();
    if (!extensionTableSources.isEmpty()) {
      stmt.getTableSources().addAll(extensionTableSources);
      query = SQLStatementUtils.toSQLString(stmt);
    }

    submitQueryToBackendDatabase(request.getConnectionId(), query)
        .then(
            (result) -> {
              var context = newCommandExecutionContext();
              for (var command : dropTableCommands) {
                command.execute(context);
              }
              callback.onSuccess(new WrappedHandlerResult(result));
              return true;
            })
        .doCatch(
            (e) -> {
              // It is a single drop table statement and the table does not exist.
              if (stmt.getTableSources().size() == 1 && SQLErrorUtils.isBadTable(e)) {
                var context = newCommandExecutionContext();
                for (var command : dropTableCommands) {
                  command.execute(context);
                }
              }
              callback.onFailure(e);
              return false;
            });
  }
}
