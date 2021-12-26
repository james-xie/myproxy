package com.gllue.myproxy.command.handler.query.ddl.create;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.TestHelper;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.FuturableCallback;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateTableHandlerTest extends BaseQueryHandlerTest {
  @Mock PersistRepository repository;

  public CreateTableHandler newHandler() {
    return new CreateTableHandler(
        repository, configurations, clusterState, transportService, sqlParser);
  }

  protected void mockConfigurations() {
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE))
        .thenReturn(100);
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK))
        .thenReturn(0.9);
    when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn(ROOT_PATH);
  }

  @Test
  public void testExecuteDirectly() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

    var request = newQueryHandlerRequest(query, Map.of());

    var future = new PlainFuture<>();
    var callback =
        new Callback<HandlerResult>() {
          @Override
          public void onSuccess(HandlerResult result) {
            future.set(result);
          }

          @Override
          public void onFailure(Throwable e) {
            future.setException(e);
          }
        };

    var handler = newHandler();
    handler.execute(request, callback);

    future.get();

    assertEquals(1, submitSqlList.size());
    assertSQLEquals(query, submitSqlList.get(0));
  }

  @Test
  public void testExecuteWithEncryptAndPartition() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_MAX_COLUMNS_PER_TABLE))
        .thenReturn(5);
    when(configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.EXTENSION_TABLE_COLUMNS_ALLOCATION_WATERMARK))
        .thenReturn(1.0);

    Map<SQLCommentAttributeKey, Object> attributes =
        Map.of(
            SQLCommentAttributeKey.PARTITION_TABLE,
            true,
            SQLCommentAttributeKey.EXTENSION_COLUMNS,
            new String[] {
              "e_col_1", "e_col_2", "e_col_3", "e_col_4", "e_col_5", "e_col_6", "e_col_7"
            });

    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` ENCRYPT(200) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  `e_col_1` INT(11) NOT NULL,\n"
            + "  `e_col_2` VARCHAR(255) NOT NULL,\n"
            + "  `e_col_3` LONGTEXT NULL,\n"
            + "  `e_col_4` LONGTEXT NULL,\n"
            + "  `e_col_5` VARCHAR(255) NULL,\n"
            + "  `e_col_6` ENCRYPT(100) NULL,\n"
            + "  `e_col_7` LONGTEXT NULL,\n"
            + "  primary key (`id`),\n"
            + "  KEY `table_p_col_1` (`p_col_1`),\n"
            + "  KEY `table_p_col_1_2` (`p_col_1`, `p_col_2`),\n"
            + "  KEY `table_e_col_1` (`e_col_1`),\n"
            + "  KEY `table_e_col_2` (`e_col_2`),\n"
            + "  UNIQUE KEY `table_p_col_1_3` (`p_col_1`, `p_col_3`),\n"
            + "  CONSTRAINT `fk_table_col_1` FOREIGN KEY (`e_col_1`) REFERENCES `ext_table` (`id`) \n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

    var request = newQueryHandlerRequest(query, attributes);
    var callback = new FuturableCallback<HandlerResult>();
    var metaDataBytes = new AtomicReference<byte[]>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              metaDataBytes.set((byte[]) args[1]);
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(3, submitSqlList.size());
    assertSQLEquals(
        "CREATE TABLE `table` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`id` INT NOT NULL AUTO_INCREMENT,\n"
            + "`p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "`p_col_2` VARBINARY(200) NOT NULL,\n"
            + "`p_col_3` INT(11) NOT NULL,\n"
            + "PRIMARY KEY (`id`),\n"
            + "KEY `$_key_for_ext_id` (`$_ext_id`),\n"
            + "KEY `table_p_col_1` (`p_col_1`),\n"
            + "KEY `table_p_col_1_2` (`p_col_1`, `p_col_2`),\n"
            + "UNIQUE KEY `table_p_col_1_3` (`p_col_1`, `p_col_3`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        submitSqlList.get(0));

    assertSQLEquals(
        "CREATE TABLE `$e_table_1` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_1` INT(11) NOT NULL,\n"
            + "`e_col_2` VARCHAR(255) NOT NULL,\n"
            + "`e_col_3` LONGTEXT NULL,\n"
            + "`e_col_4` LONGTEXT NULL,\n"
            + "`e_col_5` VARCHAR(255) NULL,\n"
            + "PRIMARY KEY (`$_ext_id`),\n"
            + "KEY `table_e_col_1` (`e_col_1`),\n"
            + "KEY `table_e_col_2` (`e_col_2`),\n"
            + "CONSTRAINT `fk_table_col_1` FOREIGN KEY (`e_col_1`) REFERENCES `ext_table` (`id`)"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        submitSqlList.get(1));

    assertSQLEquals(
        "CREATE TABLE `$e_table_2` (\n"
            + "`$_ext_id` BIGINT NOT NULL COMMENT 'Primary key of the extension table.',\n"
            + "`e_col_6` VARBINARY(100) NULL,\n"
            + "`e_col_7` LONGTEXT NULL,\n"
            + "PRIMARY KEY (`$_ext_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci",
        submitSqlList.get(2));

    // Verify the correctness of the metadata
    verify(repository).save(anyString(), any(byte[].class));
    var table = TestHelper.bytesToPartitionTableMetaData(metaDataBytes.get());
    Assert.assertEquals("table", table.getName());
    Assert.assertEquals(3, table.getNumberOfTables());
    Assert.assertEquals(
        List.of("$_ext_id", "id", "p_col_1", "p_col_2", "p_col_3"),
        table.getTableByOrdinalValue(0).getColumnNames());
    Assert.assertEquals(
        List.of("$_ext_id", "e_col_1", "e_col_2", "e_col_3", "e_col_4", "e_col_5"),
        table.getTableByOrdinalValue(1).getColumnNames());
    Assert.assertEquals(
        List.of("$_ext_id", "e_col_6", "e_col_7"),
        table.getTableByOrdinalValue(2).getColumnNames());
    Assert.assertEquals(ColumnType.ENCRYPT, table.getColumn("p_col_2").getType());
    Assert.assertEquals(ColumnType.ENCRYPT, table.getColumn("e_col_6").getType());
  }

  @Test
  public void testExecuteWithEncrypt() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    Map<SQLCommentAttributeKey, Object> attributes = Map.of();

    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` ENCRYPT(200) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

    var request = newQueryHandlerRequest(query, attributes);
    var callback = new FuturableCallback<HandlerResult>();
    var metaDataBytes = new AtomicReference<byte[]>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              metaDataBytes.set((byte[]) args[1]);
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(1, submitSqlList.size());
    assertSQLEquals(
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` VARBINARY(200) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;",
        submitSqlList.get(0));

    // Verify the correctness of the metadata
    verify(repository).save(anyString(), any(byte[].class));
    var table = TestHelper.bytesToTableMetaData(metaDataBytes.get());
    Assert.assertEquals("table", table.getName());
    Assert.assertEquals(List.of("id", "p_col_1", "p_col_2", "p_col_3"), table.getColumnNames());
    Assert.assertEquals(ColumnType.ENCRYPT, table.getColumn("p_col_2").getType());
  }

  @Test
  public void testExecuteWithCreateFailed() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));

    var submitSqlList = new ArrayList<String>();
    AtomicInteger counter = new AtomicInteger();
    Function<String, CommandResult> sqlHandler =
        (sql) -> {
          submitSqlList.add(sql);
          if (counter.getAndIncrement() == 0) {
            throw new BackendResultReadException(MySQLServerErrorCode.ER_TABLE_EXISTS_ERROR);
          }
          return emptyCommandResult();
        };
    mockTransportService(sqlHandler);

    Map<SQLCommentAttributeKey, Object> attributes = Map.of();

    var query =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` ENCRYPT(200) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";

    var request = newQueryHandlerRequest(query, attributes);
    var callback = new FuturableCallback<HandlerResult>();
    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(3, submitSqlList.size());
    var expectedQuery =
        "CREATE TABLE `table` (\n"
            + "  `id` INT NOT NULL AUTO_INCREMENT,\n"
            + "  `p_col_1` VARCHAR(50) DEFAULT 'no type',\n"
            + "  `p_col_2` VARBINARY(200) NOT NULL,\n"
            + "  `p_col_3` INT(11) NOT NULL,\n"
            + "  primary key (`id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
    assertSQLEquals(expectedQuery, submitSqlList.get(0));

    assertSQLEquals("DROP TABLE IF EXISTS `table`", submitSqlList.get(1));

    assertSQLEquals(expectedQuery, submitSqlList.get(2));
  }
}
