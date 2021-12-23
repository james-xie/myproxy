package com.gllue.myproxy.command.handler.query.ddl.alter;

import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRename;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.query.DefaultHandlerResult;
import com.gllue.myproxy.command.handler.query.EncryptColumnHelper;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.ddl.AbstractDDLHandler;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.exception.BadDatabaseException;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.command.AbstractTableUpdateCommand;
import com.gllue.myproxy.metadata.command.AbstractTableUpdateCommand.Column;
import com.gllue.myproxy.metadata.command.UpdatePartitionTableCommand;
import com.gllue.myproxy.metadata.command.UpdatePartitionTableCommand.Table;
import com.gllue.myproxy.metadata.command.UpdateTableCommand;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AlterTableHandler extends AbstractDDLHandler {
  private static final String NAME = "Alter table handler";

  public AlterTableHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<DefaultHandlerResult> callback) {
    ensureDatabaseExists(request);

    var stmt = (SQLAlterTableStatement) request.getStatement();
    var datasource = request.getDatasource();
    var database = request.getDatabase();
    var tableName = SQLStatementUtils.unquoteName(stmt.getTableName());

    var databaseMetaData = clusterState.getMetaData().getDatabase(datasource, database);
    if (databaseMetaData == null) {
      throw new BadDatabaseException(database);
    }

    var tableMetaData = databaseMetaData.getTable(tableName);
    if (tableMetaData == null) {
      executeAlterTable(request, callback, tableName);
    } else if (tableMetaData.getType() == TableType.STANDARD) {
      executeAlterTableOnStandardTable(request, callback, tableMetaData);
    } else if (tableMetaData.getType() == TableType.PARTITION) {
      executeAlterTableOnPartitionTable(request, callback, (PartitionTableMetaData) tableMetaData);
    } else {
      throw new IllegalStateException();
    }
  }

  @RequiredArgsConstructor
  class AlterTableExecutor {
    private final QueryHandlerRequest request;
    private final TableMetaData tableMetaData;
    private final SQLAlterTableStatement statement;
    private final EncryptColumnProcessor encryptColumnProcessor;
    private final AlterTableStatementProcessor alterTableProcessor;

    public AlterTableExecutor(
        final QueryHandlerRequest request,
        final TableMetaData tableMetaData,
        final SQLAlterTableStatement statement,
        final Map<String, SQLColumnDefinition> columnsInDatabase) {
      this.request = request;
      this.tableMetaData = tableMetaData;
      this.statement = statement;
      this.encryptColumnProcessor =
          new EncryptColumnProcessor(EncryptColumnHelper.getEncryptKey(request));
      this.alterTableProcessor =
          new AlterTableStatementProcessor(
              tableMetaData, columnsInDatabase, encryptColumnProcessor);
    }

    private Promise<CommandResult> encryptAndDecryptData() {
      var tableName = tableMetaData.getName();
      var updateSql = encryptColumnProcessor.generateUpdateSqlToEncryptAndDecryptData(tableName);
      if (updateSql == null) {
        return Promise.emptyPromise();
      }
      return submitQueryToBackendDatabase(request.getConnectionId(), updateSql);
    }

    private Promise<CommandResult> renameTemporaryEncryptColumn() {
      var tableName = tableMetaData.getName();
      var alterSql = encryptColumnProcessor.generateAlterSqlToRenameTemporaryColumn(tableName);
      return submitQueryToBackendDatabase(request.getConnectionId(), alterSql);
    }

    private Promise<CommandResult> execute() {
      var newStmt = alterTableProcessor.processStatement(statement);
      encryptColumnProcessor.ensureEncryptKey();

      Promise<CommandResult> alterTablePromise;
      if (newStmt.getItems().isEmpty()) {
        alterTablePromise = Promise.emptyPromise();
      } else {
        alterTablePromise =
            submitQueryToBackendDatabase(
                request.getConnectionId(), SQLStatementUtils.toSQLString(newStmt));
      }

      if (!encryptColumnProcessor.shouldDoEncryptOrDecrypt()) {
        return alterTablePromise;
      }
      return alterTablePromise
          .thenAsync((v) -> encryptAndDecryptData())
          .thenAsync((v) -> renameTemporaryEncryptColumn());
    }
  }

  private Promise<CommandResult> doExecuteAlterTable(
      QueryHandlerRequest request,
      TableMetaData tableMetaData,
      SQLAlterTableStatement stmt,
      Map<String, SQLColumnDefinition> columnDefinitionMap) {
    return new AlterTableExecutor(request, tableMetaData, stmt, columnDefinitionMap).execute();
  }

  private String applyChangesToColumnsMap(
      String tableName, Map<String, Column> columnsMap, SQLAlterTableStatement stmt) {
    for (var item : stmt.getItems()) {
      if (item instanceof SQLAlterTableAddColumn) {
        var columnDef = ((SQLAlterTableAddColumn) item).getColumns().get(0);
        var columnName = SQLStatementUtils.unquoteName(columnDef.getColumnName());
        var old = columnsMap.putIfAbsent(columnName, buildCommandColumn(columnDef));
        if (old != null) {
          var message =
              "Column [{}.{}] is already in the table metadata, ignore SQLAlterTableAddColumn item.";
          log.error(message, tableName, old);
        }
      } else if (item instanceof MySqlAlterTableModifyColumn) {
        var columnDef = ((MySqlAlterTableModifyColumn) item).getNewColumnDefinition();
        var columnInfo = buildCommandColumn(columnDef);
        columnsMap.put(columnInfo.name, columnInfo);
      } else if (item instanceof MySqlAlterTableChangeColumn) {
        var changeItem = (MySqlAlterTableChangeColumn) item;
        var columnDef = changeItem.getNewColumnDefinition();
        var columnInfo = buildCommandColumn(columnDef);
        columnsMap.remove(
            SQLStatementUtils.unquoteName(changeItem.getColumnName().getSimpleName()));
        columnsMap.put(columnInfo.name, columnInfo);
      } else if (item instanceof SQLAlterTableDropColumnItem) {
        var sqlName = ((SQLAlterTableDropColumnItem) item).getColumns().get(0);
        columnsMap.remove(SQLStatementUtils.unquoteName(sqlName.getSimpleName()));
      } else if (item instanceof SQLAlterTableRename) {
        var renameItem = (SQLAlterTableRename) item;
        tableName = SQLStatementUtils.unquoteName(renameItem.getToName().getSimpleName());
      }
    }
    return tableName;
  }

  private boolean updateStandardTableMetaData(
      QueryHandlerRequest request, TableMetaData table, SQLAlterTableStatement stmt) {
    var tableName = table.getName();
    Map<String, UpdateTableCommand.Column> columnsMap = new HashMap<>();
    for (int i = 0; i < table.getNumberOfColumns(); i++) {
      var column = table.getColumn(i);
      columnsMap.put(column.getName(), AbstractTableUpdateCommand.Column.newColumn(column));
    }

    tableName = applyChangesToColumnsMap(tableName, columnsMap, stmt);

    new UpdateTableCommand(
            request.getDatasource(),
            request.getDatabase(),
            table.getIdentity(),
            tableName,
            columnsMap.values().toArray(Column[]::new))
        .execute(newCommandExecutionContext());
    return true;
  }

  private boolean updatePartitionTableMetaData(
      QueryHandlerRequest request,
      PartitionTableMetaData table,
      List<MySqlCreateTableStatement> createTableStmtList,
      List<SQLAlterTableStatement> alterTableStmtList) {
    var tableCount = table.getNumberOfTables() + createTableStmtList.size();
    Table[] tables = new Table[tableCount];

    Map<String, SQLAlterTableStatement> alterTableStmtMap = new HashMap<>();
    for (var stmt : alterTableStmtList) {
      alterTableStmtMap.put(SQLStatementUtils.unquoteName(stmt.getTableName()), stmt);
    }

    int i = 0;
    for (; i < table.getNumberOfTables(); i++) {
      var subTable = table.getTableByOrdinalValue(i);
      var tableName = subTable.getName();
      Map<String, UpdateTableCommand.Column> columnsMap = new HashMap<>();
      for (int j = 0; j < subTable.getNumberOfColumns(); j++) {
        var column = subTable.getColumn(i);
        columnsMap.put(column.getName(), AbstractTableUpdateCommand.Column.newColumn(column));
      }

      var alterTableStmt = alterTableStmtMap.get(tableName);
      if (alterTableStmt != null) {
        tableName = applyChangesToColumnsMap(tableName, columnsMap, alterTableStmt);
      }
      tables[i] = new Table(tableName, columnsMap.values().toArray(Column[]::new));
    }

    for (var stmt : createTableStmtList) {
      var tableName = SQLStatementUtils.unquoteName(stmt.getTableName());
      var columnDefs = stmt.getColumnDefinitions();
      var columns = new AbstractTableUpdateCommand.Column[columnDefs.size()];
      for (int j = 0; j < columns.length; j++) {
        var columnDef = columnDefs.get(j);
        columns[j] = buildCommandColumn(columnDef);
      }
      tables[i++] = new Table(tableName, columns);
    }

    new UpdatePartitionTableCommand(
            request.getDatasource(),
            request.getDatabase(),
            table.getIdentity(),
            tables[0].name,
            tables[0],
            Arrays.copyOfRange(tables, 1, tables.length))
        .execute(newCommandExecutionContext());
    return true;
  }

  private Promise<Map<String, SQLColumnDefinition>> rebuildTableMetaData(
      QueryHandlerRequest request, String tableName) {
    return showCreateTable(request, tableName)
        .then(
            (createTableQuery) -> {
              var createTableStmt = (MySqlCreateTableStatement) sqlParser.parse(createTableQuery);
              var command =
                  buildCreateStandardTableCommand(
                      request.getDatasource(), request.getDatabase(), tableName, createTableStmt);
              command.execute(newCommandExecutionContext());
              return createTableStmt.getColumnDefinitions().stream()
                  .collect(
                      Collectors.toMap(
                          x -> SQLStatementUtils.unquoteName(x.getColumnName()), x -> x));
            });
  }

  private void executeAlterTable(
      QueryHandlerRequest request, Callback<DefaultHandlerResult> callback, String tableName) {
    var stmt = (SQLAlterTableStatement) request.getStatement();
    var encryptColumnProcessor =
        new EncryptColumnProcessor(EncryptColumnHelper.getEncryptKey(request));

    var isAlterEncryptColumn = false;
    for (var item : stmt.getItems()) {
      isAlterEncryptColumn |=
          encryptColumnProcessor.isAddEncryptColumn(item)
              || encryptColumnProcessor.isModifyToEncryptColumn(item)
              || encryptColumnProcessor.isChangeToEncryptColumn(item);
    }
    if (!isAlterEncryptColumn) {
      submitQueryToBackendDatabase(request, request.getQuery(), callback);
      return;
    }

    Function<CommandResult, Promise<Boolean>> operation =
        (r) -> {
          return rebuildTableMetaData(request, tableName)
              .thenAsync(
                  (columnDefinitionMap) -> {
                    // TODO: maybe cannot get table metadata.
                    var table =
                        clusterState
                            .getMetaData()
                            .getDatabase(request.getDatasource(), request.getDatabase())
                            .getTable(tableName);
                    return doExecuteAlterTable(request, table, stmt, columnDefinitionMap)
                        .then((v) -> updateStandardTableMetaData(request, table, stmt));
                  });
        };

    // lock table
    lockTables(request.getConnectionId(), operation, LockType.WRITE, tableName)
        .then(
            (v) -> {
              callback.onSuccess(DefaultHandlerResult.getInstance());
              return true;
            },
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }

  void executeAlterTableOnStandardTable(
      QueryHandlerRequest request, Callback<DefaultHandlerResult> callback, TableMetaData table) {
    var tableName = table.getName();
    var stmt = (SQLAlterTableStatement) request.getStatement();

    Function<CommandResult, Promise<Boolean>> operation =
        (r) -> {
          return showCreateTableReturnColumnDefMap(request, tableName)
              .thenAsync(
                  (columnDefinitionMap) -> {
                    return doExecuteAlterTable(request, table, stmt, columnDefinitionMap)
                        .then((v) -> updateStandardTableMetaData(request, table, stmt));
                  });
        };

    // lock table
    lockTables(request.getConnectionId(), operation, LockType.WRITE, tableName)
        .then(
            (v) -> {
              callback.onSuccess(DefaultHandlerResult.getInstance());
              return true;
            },
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }

  private Function<CommandResult, Promise<CommandResult>> alterTablePromiseSupplier(
      QueryHandlerRequest request,
      PartitionTableMetaData partitionTable,
      List<SQLAlterTableStatement> statements) {
    var index = new AtomicInteger(statements.size());
    return (result) -> {
      var i = index.decrementAndGet();
      if (i < 0) return null;

      var stmt = statements.get(i);
      var table = partitionTable.getTableByOrdinalValue(i);
      var tableName = table.getName();
      return showCreateTableReturnColumnDefMap(request, tableName)
          .thenAsync(
              (columnDefinitionMap) -> {
                return doExecuteAlterTable(request, table, stmt, columnDefinitionMap);
              });
    };
  }

  void executeAlterTableOnPartitionTable(
      QueryHandlerRequest request,
      Callback<DefaultHandlerResult> callback,
      PartitionTableMetaData table) {
    var stmt = (SQLAlterTableStatement) request.getStatement();

    var tablePartitionProcessor =
        new TablePartitionProcessor(configurations, table, request.getCommentsAttributes());
    tablePartitionProcessor.validateStatement(stmt);

    Function<CommandResult, Promise<Boolean>> operation =
        (r) -> {
          var excludeItems = new HashSet<SQLAlterTableItem>();
          var items = stmt.getItems();
          var tableOptionItems = tablePartitionProcessor.extractTableOptionItems(items);
          var createTableStmtList =
              tablePartitionProcessor.prepareCreateNewExtensionTables(
                  items, tableOptionItems, excludeItems);
          var alterTableStmtList =
              tablePartitionProcessor.partitionStatement(items, tableOptionItems, excludeItems);
          return Promise.chain(createTablePromiseSupplier(request, createTableStmtList))
              .thenAsync(
                  (v) -> {
                    return Promise.chain(
                        alterTablePromiseSupplier(request, table, alterTableStmtList));
                  })
              .then(
                  (v) -> {
                    return updatePartitionTableMetaData(
                        request, table, createTableStmtList, alterTableStmtList);
                  });
        };

    // lock table
    lockTables(request.getConnectionId(), operation, LockType.WRITE, table.getTableNames())
        .then(
            (v) -> {
              callback.onSuccess(DefaultHandlerResult.getInstance());
              return true;
            },
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
