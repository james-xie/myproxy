package com.gllue.myproxy.command.handler.query.ddl.alter;

import static com.gllue.myproxy.common.util.SQLStatementUtils.isSQLEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.TestHelper;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.FuturableCallback;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.metadata.model.ColumnMetaData;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.repository.PersistRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AlterTableHandlerTest extends BaseQueryHandlerTest {
  @Mock PersistRepository repository;

  public AlterTableHandler newHandler() {
    return new AlterTableHandler(
        repository, configurations, clusterState, transportService, sqlParser, threadPool);
  }

  protected void mockConfigurations() {
    when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn(ROOT_PATH);
    when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.ENCRYPTION_ALGORITHM))
        .thenReturn("AES");
  }

  @Test
  public void testExecuteDirectly() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    var query =
        "ALTER TABLE `table1`\n"
            + " ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT, \n"
            + " ADD COLUMN `value` varchar(255) NOT NULL, \n"
            + " ADD COLUMN `create_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00', \n"
            + " MODIFY COLUMN `name` varchar(255) NOT NULL, \n"
            + " CHANGE COLUMN `name1` `name2` int(11) NOT NULL;";

    var request = newQueryHandlerRequest(query, Map.of());

    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(1, submitSqlList.size());
    assertSQLEquals(query, submitSqlList.get(0));
  }

  @Test
  public void testAlterTableWithTableMetaDataExists()
      throws InterruptedException, ExecutionException {
    mockConfigurations();
    var table1 = prepareTable("table1", "id", "name", "name1", "name3");
    mockClusterState(prepareMultiDatabasesMetaData(table1));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    var query =
        "ALTER TABLE `table1`\n"
            + " ADD COLUMN `value` varchar(255) NOT NULL, \n"
            + " ADD COLUMN `create_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00', \n"
            + " MODIFY COLUMN `name` varchar(255) NULL, \n"
            + " CHANGE COLUMN `name1` `name2` int(11) NULL;";

    var metaDataBytes = new AtomicReference<byte[]>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              metaDataBytes.set((byte[]) args[1]);
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    Function<String, CommandResult> sqlHandler =
        (sql) -> {
          submitSqlList.add(sql);
          if (isSQLEquals("show create table `table1`", sql)) {
            return newCommandResult(
                List.of("Table", "Create Table"),
                new String[][] {
                  new String[] {
                    "table1",
                    "CREATE TABLE `table1` (\n"
                        + "  `id` varchar(255) NOT NULL,\n"
                        + "  `name` varchar(255) NOT NULL,\n"
                        + "  `name1` varchar(255) NOT NULL,\n"
                        + "  `name3` varchar(255) NOT NULL,\n"
                        + "  PRIMARY KEY (`id`)\n"
                        + ") ENGINE=InnoDB"
                  }
                });
          }
          return emptyCommandResult();
        };
    mockTransportService(sqlHandler);

    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();
    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(4, submitSqlList.size());
    assertSQLEquals(query, submitSqlList.get(2));

    // Verify the correctness of the metadata
    verify(repository).save(anyString(), any(byte[].class));
    var table = TestHelper.bytesToTableMetaData(metaDataBytes.get());
    Assert.assertEquals("table1", table.getName());
    Assert.assertEquals(
        List.of("id", "name", "name3", "value", "create_time", "name2"), table.getColumnNames());

    var name = table.getColumn("name");
    var name1 = table.getColumn("name1");
    var name2 = table.getColumn("name2");
    Assert.assertTrue(name.isNullable());
    Assert.assertNull(name1);
    Assert.assertEquals(ColumnType.INT, name2.getType());
    Assert.assertTrue(name2.isNullable());
  }

  @Test
  public void testAlterTableWithAddEncryptColumn() throws InterruptedException, ExecutionException {
    mockConfigurations();
    mockClusterState(prepareMultiDatabasesMetaData(null));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    var query =
        "ALTER TABLE `table1`\n"
            + " ADD COLUMN `value` ENCRYPT(255) NOT NULL, \n"
            + " MODIFY COLUMN `name` int(11) NOT NULL;";

    var metaDataBytes = new AtomicReference<byte[]>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              metaDataBytes.set((byte[]) args[1]);
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    Function<String, CommandResult> sqlHandler =
        (sql) -> {
          submitSqlList.add(sql);
          if (isSQLEquals("show create table `table1`", sql)) {
            return newCommandResult(
                List.of("Table", "Create Table"),
                new String[][] {
                  new String[] {
                    "table1",
                    "CREATE TABLE `table1` (\n"
                        + "  `id` varchar(255) NOT NULL,\n"
                        + "  `name` varchar(255) NOT NULL,\n"
                        + "  PRIMARY KEY (`id`)\n"
                        + ") ENGINE=InnoDB"
                  }
                });
          }
          return emptyCommandResult();
        };
    mockTransportService(sqlHandler);

    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();
    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(4, submitSqlList.size());
    assertSQLEquals(
        "ALTER TABLE `table1`\n"
            + "ADD COLUMN `value` VARBINARY(255) NOT NULL,\n"
            + "MODIFY COLUMN `name` int(11) NOT NULL",
        submitSqlList.get(2));

    // Verify the correctness of the metadata
    verify(repository, times(2)).save(anyString(), any(byte[].class));
    var table = TestHelper.bytesToTableMetaData(metaDataBytes.get());
    Assert.assertEquals("table1", table.getName());
    Assert.assertEquals(List.of("id", "name", "value"), table.getColumnNames());

    var name = table.getColumn("name");
    var value = table.getColumn("value");
    Assert.assertFalse(name.isNullable());
    Assert.assertEquals(ColumnType.INT, name.getType());
    Assert.assertFalse(value.isNullable());
    Assert.assertEquals(ColumnType.ENCRYPT, value.getType());
  }

  @Test
  public void testAlterTableWithModifyEncryptColumn()
      throws InterruptedException, ExecutionException {
    mockConfigurations();
    var builder = new TableMetaData.Builder();
    builder
        .setName("table1")
        .setType(TableType.STANDARD)
        .setIdentity(RandomUtils.randomShortUUID())
        .setVersion(1);

    builder.addColumn(
        new ColumnMetaData.Builder().setName("id").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("name").setType(ColumnType.VARCHAR).build());
    builder.addColumn(
        new ColumnMetaData.Builder().setName("value").setType(ColumnType.ENCRYPT).build());
    var table1 = builder.build();
    mockClusterState(prepareMultiDatabasesMetaData(table1));
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);
    mockEncryptKey(ENCRYPT_KEY);

    var query =
        "ALTER TABLE `table1`\n"
            + " ADD COLUMN `name1` ENCRYPT(255) NULL,\n"
            + " MODIFY COLUMN `name` ENCRYPT(255) NOT NULL,\n"
            + " MODIFY COLUMN `value` VARCHAR(255) NULL;";

    var metaDataBytes = new AtomicReference<byte[]>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              metaDataBytes.set((byte[]) args[1]);
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    Function<String, CommandResult> sqlHandler =
        (sql) -> {
          submitSqlList.add(sql);
          if (isSQLEquals("show create table `table1`", sql)) {
            return newCommandResult(
                List.of("Table", "Create Table"),
                new String[][] {
                  new String[] {
                    "table1",
                    "CREATE TABLE `table1` (\n"
                        + "  `id` varchar(255) NOT NULL,\n"
                        + "  `name` varchar(255) NOT NULL,\n"
                        + "  `value` varbinary(255) NOT NULL,\n"
                        + "  PRIMARY KEY (`id`)\n"
                        + ") ENGINE=InnoDB"
                  }
                });
          }
          return emptyCommandResult();
        };
    mockTransportService(sqlHandler);

    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();
    var handler = newHandler();
    handler.execute(request, callback);

    callback.get();

    assertEquals(6, submitSqlList.size());
    assertSQLEquals(
        "ALTER TABLE `table1` "
            + "ADD COLUMN `name1` VARBINARY(255) NULL, "
            + "ADD COLUMN `$tmp_name` VARBINARY(255) NOT NULL,"
            + "ADD COLUMN `$tmp_value` VARCHAR(255) NULL,",
        submitSqlList.get(2));
    assertSQLEquals(
        "UPDATE `table1` "
            + "SET `$tmp_name` = AES_ENCRYPT(`name`, 'key'), "
            + "`$tmp_value` = CONVERT(AES_DECRYPT(`value`, 'key') USING 'utf8mb4') ",
        submitSqlList.get(3));
    assertSQLEquals(
        "ALTER TABLE `table1`\n"
            + "DROP COLUMN `name`,\n"
            + "CHANGE COLUMN `$tmp_name` `name` VARBINARY(255) NOT NULL,\n"
            + "DROP COLUMN `value`,\n"
            + "CHANGE COLUMN `$tmp_value` `value` VARCHAR(255) NULL",
        submitSqlList.get(4));

    // Verify the correctness of the metadata
    verify(repository, times(1)).save(anyString(), any(byte[].class));

    var table = TestHelper.bytesToTableMetaData(metaDataBytes.get());
    Assert.assertEquals("table1", table.getName());
    Assert.assertEquals(List.of("id", "name", "value", "name1"), table.getColumnNames());

    var name = table.getColumn("name");
    var name1 = table.getColumn("name1");
    var value = table.getColumn("value");
    Assert.assertFalse(name.isNullable());
    Assert.assertEquals(ColumnType.ENCRYPT, name.getType());
    Assert.assertTrue(name1.isNullable());
    Assert.assertEquals(ColumnType.ENCRYPT, name1.getType());
    Assert.assertTrue(value.isNullable());
    Assert.assertEquals(ColumnType.VARCHAR, value.getType());
  }

  //  @Test
  //  public void testAlterPartitionTable() throws InterruptedException, ExecutionException {
  //    mockConfigurations();
  //    mockClusterState(prepareMultiDatabasesMetaData(null));
  //    var submitSqlList = new ArrayList<String>();
  //    mockTransportService(submitSqlList);
  //
  //  }
  //
  //  @Test
  //  public void testAlterPartitionTableWithEncryptColumn() throws InterruptedException,
  // ExecutionException {
  //    mockConfigurations();
  //    mockClusterState(prepareMultiDatabasesMetaData(null));
  //    var submitSqlList = new ArrayList<String>();
  //    mockTransportService(submitSqlList);
  //
  //  }
  //
  //  @Test
  //  public void testCreateNewExtensionTableBeforeAlterTable() throws InterruptedException,
  // ExecutionException {
  //    mockConfigurations();
  //    mockClusterState(prepareMultiDatabasesMetaData(null));
  //    var submitSqlList = new ArrayList<String>();
  //    mockTransportService(submitSqlList);
  //
  //  }

}
