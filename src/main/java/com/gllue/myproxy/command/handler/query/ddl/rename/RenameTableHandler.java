package com.gllue.myproxy.command.handler.query.ddl.rename;

import static com.gllue.myproxy.command.handler.query.TablePartitionHelper.generateExtensionTableName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.toSQLString;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.WrappedHandlerResult;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.exception.TableExistsException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.RenameTableCommand;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;
import java.util.List;

public class RenameTableHandler extends AbstractDDLHandler {
  private static final String NAME = "Rename table handler";

  public RenameTableHandler(
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

  private String getSchema(SQLName name, String defaultSchema) {
    if (name instanceof SQLPropertyExpr) {
      return unquoteName(((SQLPropertyExpr) name).getOwnerName());
    }
    return defaultSchema;
  }

  private String getTableName(SQLName name) {
    return unquoteName(name.getSimpleName());
  }

  private List<Item> newItemsForExtensionTable(
      PartitionTableMetaData table, String oldSchema, String newSchema, String newTableName) {
    var items = new ArrayList<Item>();
    for (int i = 1; i <= table.getNumberOfExtensionTables(); i++) {
      var item = new Item();
      var extensionTable = table.getTableByOrdinalValue(i);
      item.setName(new SQLPropertyExpr(quoteName(oldSchema), quoteName(extensionTable.getName())));
      var newExtTableName = generateExtensionTableName(newTableName, i);
      item.setTo(new SQLPropertyExpr(quoteName(newSchema), quoteName(newExtTableName)));
      items.add(item);
    }
    return items;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var stmt = (MySqlRenameTableStatement) request.getStatement();
    var datasource = request.getDatasource();
    var databases = clusterState.getMetaData();

    var newItems = new ArrayList<Item>();
    var commands = new ArrayList<RenameTableCommand>();
    for (var item : stmt.getItems()) {
      String schema = getSchema(item.getName(), request.getDatabase());
      String tableName = getTableName(item.getName());

      var database = databases.getDatabase(datasource, schema);
      if (database != null && database.hasTable(tableName)) {
        var table = database.getTable(tableName);
        var newSchema = getSchema(item.getTo(), request.getDatabase());
        var newTableName = getTableName(item.getTo());
        var newDatabase = databases.getDatabase(datasource, newSchema);
        if (newDatabase != null && newDatabase.hasTable(newTableName)) {
          throw new TableExistsException(newTableName);
        }

        if (table.getType() == TableType.PARTITION) {
          newItems.addAll(
              newItemsForExtensionTable(
                  (PartitionTableMetaData) table, schema, newSchema, newTableName));
        }
        commands.add(
            new RenameTableCommand(datasource, schema, newSchema, tableName, newTableName));
      }
    }

    var query = request.getQuery();
    if (!newItems.isEmpty()) {
      stmt.getItems().addAll(newItems);
      query = toSQLString(stmt);
    }

    submitQueryToBackendDatabase(request.getConnectionId(), query)
        .then(
            (result) -> {
              if (!commands.isEmpty()) {
                var context = newCommandExecutionContext();
                for (var command : commands) {
                  command.execute(context);
                }
              }

              callback.onSuccess(new WrappedHandlerResult(result));
              return true;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
