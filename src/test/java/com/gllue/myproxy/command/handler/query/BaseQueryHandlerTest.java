package com.gllue.myproxy.command.handler.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.myproxy.AssertUtils;
import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData;
import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData.Column;
import com.gllue.myproxy.command.result.query.QueryResultMetaData;
import com.gllue.myproxy.command.result.query.SimpleQueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.FuturableCallback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.concurrent.ThreadPool.Name;
import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.core.service.TransportService;
import com.gllue.myproxy.transport.frontend.connection.SessionContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.mockito.Mock;

public abstract class BaseQueryHandlerTest {
  protected static final int FRONTEND_CONNECTION_ID = 1;
  protected static final int BACKEND_CONNECTION_ID = 2;
  protected static final String DATASOURCE = "ds";
  protected static final String DATABASE = "db";
  protected static final String ROOT_PATH = "/root";
  protected static final String ENCRYPT_KEY = "key";

  @Mock protected PersistRepository repository;

  @Mock protected Configurations configurations;

  @Mock protected ClusterState clusterState;

  @Mock protected TransportService transportService;

  @Mock protected ThreadPool threadPool;

  @Mock protected SessionContext sessionContext;

  protected final SQLParser sqlParser = new SQLParser();

  protected TableMetaData prepareTable(String tableName, String... columnNames) {
    var builder = new TableMetaData.Builder();
    builder
        .setName(tableName)
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    for (var columnName : columnNames) {
      builder.addColumn(
          new ColumnMetaData.Builder().setName(columnName).setType(ColumnType.VARCHAR).build());
    }
    return builder.build();
  }

  protected PartitionTableMetaData preparePartitionTable(String tableName) {
    var primaryTable =
        new TableMetaData.Builder()
            .setName(tableName)
            .setIdentity(RandomUtils.randomShortUUID())
            .setType(TableType.PRIMARY)
            .addColumn(new ColumnMetaData.Builder().setName("id").setType(ColumnType.INT).build())
            .addColumn(
                new ColumnMetaData.Builder().setName("col1").setType(ColumnType.ENCRYPT).build())
            .addColumn(
                new ColumnMetaData.Builder().setName("col3").setType(ColumnType.VARCHAR).build())
            .addColumn(
                new ColumnMetaData.Builder()
                    .setName(TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN)
                    .setType(ColumnType.INT)
                    .setBuiltin(true)
                    .build())
            .build();
    var extensionTable =
        new TableMetaData.Builder()
            .setName(tableName + "_ext_1")
            .setIdentity(RandomUtils.randomShortUUID())
            .setType(TableType.EXTENSION)
            .addColumn(
                new ColumnMetaData.Builder().setName("col2").setType(ColumnType.ENCRYPT).build())
            .addColumn(
                new ColumnMetaData.Builder().setName("col4").setType(ColumnType.VARCHAR).build())
            .addColumn(
                new ColumnMetaData.Builder()
                    .setName(TablePartitionHelper.EXTENSION_TABLE_ID_COLUMN)
                    .setType(ColumnType.INT)
                    .setBuiltin(true)
                    .build())
            .build();

    var builder =
        new PartitionTableMetaData.Builder()
            .setName(tableName)
            .setIdentity(RandomUtils.randomShortUUID())
            .setPrimaryTable(primaryTable)
            .addExtensionTable(extensionTable);
    return builder.build();
  }

  protected DatabaseMetaData prepareDatabase(
      String datasource, String database, TableMetaData... tables) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(datasource);
    builder.setName(database);
    for (var table : tables) {
      builder.addTable(table);
    }
    return builder.build();
  }

  protected MultiDatabasesMetaData prepareMultiDatabasesMetaData(
      String datasource, String database, TableMetaData... tables) {
    var builder = new MultiDatabasesMetaData.Builder();
    var databaseMetaData = prepareDatabase(datasource, database, tables);
    builder.addDatabase(databaseMetaData);
    return builder.build();
  }

  protected MultiDatabasesMetaData prepareMultiDatabasesMetaData(TableMetaData tableMetaData) {
    if (tableMetaData == null) {
      return prepareMultiDatabasesMetaData(DATASOURCE, DATABASE);
    }
    return prepareMultiDatabasesMetaData(DATASOURCE, DATABASE, tableMetaData);
  }

  protected CommandResult emptyCommandResult() {
    return CommandResult.newEmptyResult();
  }

  protected void mockClusterState(MultiDatabasesMetaData databasesMetaData) {
    doAnswer(
            invocation -> {
              return databasesMetaData;
            })
        .when(clusterState)
        .getMetaData();
  }

  protected void mockThreadPool() {
    mockThreadPool(ThreadPool.DIRECT_EXECUTOR_SERVICE);
  }

  protected void mockThreadPool(ExecutorService executor) {
    when(threadPool.executor(Name.COMMAND)).thenReturn(executor);
    when(threadPool.executor(Name.GENERIC)).thenReturn(executor);
  }

  @SuppressWarnings("unchecked")
  protected void mockSubmitQueryToBackendDatabase(Function<String, CommandResult> sqlHandler) {
    doAnswer(
            invocation -> {
              var args = invocation.getArguments();
              var callback = (Callback<CommandResult>) args[2];
              try {
                var res = sqlHandler.apply((String) args[1]);
                callback.onSuccess(res);
              } catch (Exception e) {
                callback.onFailure(e);
              }
              return null;
            })
        .when(transportService)
        .submitQueryToBackendDatabase(anyInt(), anyString(), any(Callback.class));
  }

  @SuppressWarnings("unchecked")
  protected void mockSubmitQueryToBackendDatabase(List<String> sqlCollector) {
    mockSubmitQueryToBackendDatabase(
        (x) -> {
          sqlCollector.add(x);
          return emptyCommandResult();
        });
  }

  @SuppressWarnings("unchecked")
  protected void mockSubmitQueryAndDirectTransferResult(
      Function<String, CommandResult> sqlHandler) {
    doAnswer(
            invocation -> {
              var args = invocation.getArguments();
              var callback = (Callback<CommandResult>) args[2];
              try {
                var res = sqlHandler.apply((String) args[1]);
                callback.onSuccess(res);
              } catch (Exception e) {
                callback.onFailure(e);
              }
              return null;
            })
        .when(transportService)
        .submitQueryAndDirectTransferResult(anyInt(), anyString(), any(Callback.class));
  }

  @SuppressWarnings("unchecked")
  protected void mockSubmitQueryAndDirectTransferResult(List<String> sqlCollector) {
    mockSubmitQueryAndDirectTransferResult(
        (x) -> {
          sqlCollector.add(x);
          return emptyCommandResult();
        });
  }

  protected void mockEncryptKey(String encryptKey) {
    when(sessionContext.getEncryptKey()).thenReturn(encryptKey);
  }

  protected MySqlCreateTableStatement parseCreateTableQuery(final String query) {
    return (MySqlCreateTableStatement) sqlParser.parse(query);
  }

  protected SQLAlterTableStatement parseAlterTableQuery(final String query) {
    return (SQLAlterTableStatement) sqlParser.parse(query);
  }

  protected SQLSelectStatement parseSelectQuery(final String query) {
    return (SQLSelectStatement) sqlParser.parse(query);
  }

  protected SQLDeleteStatement parseDeleteQuery(final String query) {
    return (SQLDeleteStatement) sqlParser.parse(query);
  }

  protected SQLUpdateStatement parseUpdateQuery(final String query) {
    return (SQLUpdateStatement) sqlParser.parse(query);
  }

  protected SQLInsertStatement parseInsertQuery(final String query) {
    return (SQLInsertStatement) sqlParser.parse(query);
  }

  protected void assertSQLEquals(final SQLStatement expected, final SQLStatement actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }

  protected void assertSQLEquals(final String expected, final SQLStatement actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }

  protected void assertSQLEquals(final String expected, final String actual) {
    AssertUtils.assertSQLEquals(expected, actual);
  }

  protected QueryHandlerRequest newQueryHandlerRequest(
      final String query, final Map<SQLCommentAttributeKey, Object> attributes) {
    var request =
        new QueryHandlerRequestImpl(
            FRONTEND_CONNECTION_ID, DATASOURCE, DATABASE, query, sessionContext);
    request.setCommentsAttributes(attributes);
    request.setStatement(sqlParser.parse(query));
    return request;
  }

  protected QueryResultMetaData newQueryResultMetaData(
      List<String> columnNames, List<MySQLColumnType> columnTypes) {
    var columns = new Column[columnNames.size()];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new Column(columnNames.get(i), columnTypes.get(i));
    }
    return new DefaultQueryResultMetaData(columns);
  }

  protected CommandResult newCommandResult(List<String> columnNames, String[][] result) {
    var columnTypes =
        columnNames.stream()
            .map(x -> MySQLColumnType.MYSQL_TYPE_STRING)
            .collect(Collectors.toList());
    return newCommandResult(columnNames, columnTypes, result);
  }

  protected CommandResult newCommandResult(
      List<String> columnNames, List<MySQLColumnType> columnTypes, String[][] result) {
    return new CommandResult(
        0,
        0,
        0,
        0,
        "",
        new SimpleQueryResult(newQueryResultMetaData(columnNames, columnTypes), result));
  }

  protected <T> T callbackGet(FuturableCallback<T> callback)
      throws InterruptedException, ExecutionException {
    try {
      return callback.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof BaseServerException) {
        throw (BaseServerException) e.getCause();
      }
      throw e;
    }
  }
}
