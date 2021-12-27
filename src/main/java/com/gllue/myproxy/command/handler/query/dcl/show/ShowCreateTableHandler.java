package com.gllue.myproxy.command.handler.query.dcl.show;

import static com.gllue.myproxy.common.util.SQLStatementUtils.getForeignKeyReferencingColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getIndexColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getPrimaryKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.getUniqueKeyColumns;
import static com.gllue.myproxy.common.util.SQLStatementUtils.quoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.unquoteName;
import static com.gllue.myproxy.common.util.SQLStatementUtils.updateVarbinaryToEncrypt;

import com.alibaba.druid.sql.ast.SQLIndex;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLForeignKeyConstraint;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLUniqueConstraint;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.BadSQLException;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.command.result.query.SingleRowQueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.common.util.SQLStatementUtils;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShowCreateTableHandler extends AbstractQueryHandler {
  public static final String NAME = "Show create table handler";
  private static final String SHOW_CREATE_TABLE_QUERY_TEMPLATE = "SHOW CREATE TABLE %s";

  private final ClusterState clusterState;
  private final SQLParser sqlParser;

  public ShowCreateTableHandler(
      TransportService transportService, ClusterState clusterState, SQLParser sqlParser) {
    super(transportService);
    this.clusterState = clusterState;
    this.sqlParser = sqlParser;
  }

  @Override
  public String name() {
    return NAME;
  }

  private boolean checkColumnExists(String[] columns, TableMetaData table) {
    for (var column : columns) {
      if (!table.hasColumn(column)) {
        return false;
      }
    }
    return true;
  }

  private List<SQLTableElement> filterTableElements(
      List<SQLTableElement> tableElementList, TableMetaData table) {
    var newElements = new ArrayList<SQLTableElement>();
    for (var element : tableElementList) {
      var shouldAdd = true;
      if (element instanceof SQLColumnDefinition) {
        var columnDef = (SQLColumnDefinition) element;
        var columnName = unquoteName(columnDef.getColumnName());
        if (!table.hasColumn(columnName)) {
          shouldAdd = false;
        } else {
          var column = table.getColumn(columnName);
          if (column.getType() == ColumnType.ENCRYPT) {
            newElements.add(updateVarbinaryToEncrypt(columnDef));
            shouldAdd = false;
          }
        }
      } else if (element instanceof SQLPrimaryKey) {
        var columns = getPrimaryKeyColumns((SQLPrimaryKey) element);
        shouldAdd = checkColumnExists(columns, table);
      } else if (element instanceof SQLUniqueConstraint) {
        var columns = getUniqueKeyColumns((SQLUniqueConstraint) element);
        shouldAdd = checkColumnExists(columns, table);
      } else if (element instanceof SQLIndex) {
        var columns = getIndexColumns((SQLIndex) element);
        shouldAdd = checkColumnExists(columns, table);
      } else if (element instanceof SQLForeignKeyConstraint) {
        var columns = getForeignKeyReferencingColumns((SQLForeignKeyConstraint) element);
        shouldAdd = checkColumnExists(columns, table);
      }

      if (shouldAdd) {
        newElements.add(element);
      }
    }

    return newElements;
  }

  private HandlerResult newResult(
      QueryResultMetaData metaData, String tableName, SQLStatement createTableStmt) {
    return new QueryHandlerResult(
        0,
        new SingleRowQueryResult(
            metaData, new String[] {tableName, SQLStatementUtils.toSQLString(createTableStmt)}));
  }

  private void handleShowCreateTableForPartitionTable(
      QueryHandlerRequest request, Callback<HandlerResult> callback, PartitionTableMetaData table) {
    var queries =
        Arrays.stream(table.getTableNames())
            .map(x -> String.format(SHOW_CREATE_TABLE_QUERY_TEMPLATE, quoteName(x)))
            .collect(Collectors.toList());

    executeQueries(request, queries)
        .then(
            (result) -> {
              QueryResultMetaData metaData = null;
              MySqlCreateTableStatement primaryTableStmt = null;
              for (var item : result) {
                var queryResult = item.getQueryResult();
                queryResult.next();
                var createTableQuery = queryResult.getStringValue(1);
                var createTableStmt = (MySqlCreateTableStatement) sqlParser.parse(createTableQuery);
                var newTableElements =
                    filterTableElements(createTableStmt.getTableElementList(), table);

                if (primaryTableStmt == null) {
                  primaryTableStmt = createTableStmt;
                  primaryTableStmt.getTableElementList().clear();
                  metaData = item.getQueryResult().getMetaData();
                }
                primaryTableStmt.getTableElementList().addAll(newTableElements);
              }

              callback.onSuccess(newResult(metaData, table.getName(), primaryTableStmt));
              return true;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }

  private void handleShowCreateTable(
      QueryHandlerRequest request, Callback<HandlerResult> callback, TableMetaData table) {
    if (table.getType() == TableType.PARTITION) {
      handleShowCreateTableForPartitionTable(request, callback, (PartitionTableMetaData) table);
      return;
    }

    submitQueryToBackendDatabase(request.getConnectionId(), request.getQuery())
        .then(
            (result) -> {
              var queryResult = result.getQueryResult();
              queryResult.next();
              var createTableQuery = queryResult.getStringValue(1);
              var createTableStmt = (MySqlCreateTableStatement) sqlParser.parse(createTableQuery);
              var newTableElements =
                  filterTableElements(createTableStmt.getTableElementList(), table);
              createTableStmt.getTableElementList().clear();
              createTableStmt.getTableElementList().addAll(newTableElements);

              callback.onSuccess(
                  newResult(queryResult.getMetaData(), table.getName(), createTableStmt));
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

    var stmt = (SQLShowCreateTableStatement) request.getStatement();
    String tableName;
    String databaseName;
    var name = stmt.getName();
    if (name != null) {
      tableName = unquoteName(stmt.getName().getSimpleName());
      if (name instanceof SQLPropertyExpr) {
        databaseName = unquoteName(((SQLPropertyExpr) name).getOwnerName());
      } else {
        databaseName = request.getDatabase();
      }
    } else {
      throw new BadSQLException("No table name.");
    }

    var database = clusterState.getMetaData().getDatabase(request.getDatasource(), databaseName);
    if (database != null && database.hasTable(tableName)) {
      handleShowCreateTable(request, callback, database.getTable(tableName));
    } else {
      submitQueryAndDirectTransferResult(request.getConnectionId(), request.getQuery(), callback);
    }
  }
}
