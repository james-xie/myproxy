package com.gllue.command.handler.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.gllue.AssertUtils;
import com.gllue.cluster.ClusterState;
import com.gllue.command.result.CommandResult;
import com.gllue.common.Callback;
import com.gllue.common.Promise;
import com.gllue.common.util.RandomUtils;
import com.gllue.config.Configurations;
import com.gllue.metadata.model.ColumnMetaData;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.DatabaseMetaData;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableMetaData;
import com.gllue.metadata.model.TableType;
import com.gllue.sql.parser.SQLCommentAttributeKey;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.core.service.TransportService;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.mockito.Mock;

public abstract class BaseQueryHandlerTest {
  protected static final int FRONTEND_CONNECTION_ID = 1;
  protected static final int BACKEND_CONNECTION_ID = 2;
  protected static final String DATASOURCE = "ds";
  protected static final String DATABASE = "db";
  protected static final String ROOT_PATH = "/root";

  @Mock protected Configurations configurations;

  @Mock protected ClusterState clusterState;

  @Mock protected TransportService transportService;

  protected final SQLParser sqlParser = new SQLParser();

  protected TableMetaData prepareTable(String tableName, String... columnNames) {
    var builder = new TableMetaData.Builder();
    builder
        .setName(tableName)
        .setType(TableType.PRIMARY)
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

  protected DatabaseMetaData prepareDatabase(String datasource, String database) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(datasource);
    builder.setName(database);
    return builder.build();
  }

  protected MultiDatabasesMetaData prepareMultiDatabasesMetaData(
      String datasource, String database, TableMetaData... tableMetaData) {
    var builder = new MultiDatabasesMetaData.Builder();
    var databaseMetaData = prepareDatabase(datasource, database);
    for (var table : tableMetaData) {
      databaseMetaData.addTable(table);
    }
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
    return new CommandResult(0, 0, 0, 0, "", null);
  }

  protected void mockClusterState(MultiDatabasesMetaData databasesMetaData) {
    doAnswer(
            invocation -> {
              return databasesMetaData;
            })
        .when(clusterState)
        .getMetaData();
  }

  @SuppressWarnings("unchecked")
  protected void mockTransportService(Function<String, CommandResult> sqlHandler) {
    doAnswer(
            invocation -> {
              var args = invocation.getArguments();
              try {
                var res = sqlHandler.apply((String) args[1]);
                return new Promise<>(
                    (cb) -> {
                      cb.onSuccess(res);
                    });
              } catch (Exception e) {
                return new Promise<>(
                    (cb) -> {
                      cb.onFailure(e);
                    });
              }
            })
        .when(transportService)
        .submitQueryToBackendDatabase(anyInt(), anyString());

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
  protected void mockTransportService(List<String> sqlCollector) {
    mockTransportService(
        (x) -> {
          sqlCollector.add(x);
          return emptyCommandResult();
        });
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
    var request = new QueryHandlerRequestImpl(FRONTEND_CONNECTION_ID, DATASOURCE, DATABASE, query);
    request.setCommentsAttributes(attributes);
    request.setStatement(sqlParser.parse(query));
    return request;
  }
}
