package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RenameTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testRenameStandardTable() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var oldTableName = "oldTable";
    var newTableName = "newTable";
    var context = buildContext();
    var command =
        new RenameTableCommand(DATASOURCE, databaseName, databaseName, oldTableName, newTableName);

    var table = prepareTable(oldTableName);
    var database = prepareDatabase(databaseName, table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newDatabase = bytesToDatabaseMetaData((byte[]) args[1]);
              var newTable = newDatabase.getTable(newTableName);

              assertFalse(newDatabase.hasTable(oldTableName));
              assertEquals(getPersistPath(database.getIdentity()), path);
              assertEquals(table.getIdentity(), newTable.getIdentity());
              assertEquals(newTableName, newTable.getName());
              assertEquals(2, newTable.getNumberOfColumns());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }

  @Test
  public void testRenamePartitionTable() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var oldTableName = "oldTable";
    var newTableName = "newTable";
    var context = buildContext();
    var command =
        new RenameTableCommand(DATASOURCE, databaseName, databaseName, oldTableName, newTableName);

    var builder =
        new PartitionTableMetaData.Builder()
            .setName(oldTableName)
            .setPrimaryTable(prepareTable(oldTableName))
            .setIdentity(RandomUtils.randomShortUUID())
            .setVersion(1);
    var table = builder.build();
    var database = prepareDatabase(databaseName, table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newDatabase = bytesToDatabaseMetaData((byte[]) args[1]);
              var newTable = newDatabase.getTable(newTableName);

              assertFalse(newDatabase.hasTable(oldTableName));
              assertEquals(getPersistPath(database.getIdentity()), path);
              assertEquals(table.getIdentity(), newTable.getIdentity());
              assertEquals(newTableName, newTable.getName());
              assertEquals(2, newTable.getNumberOfColumns());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }

  @Test
  public void testRenameTableCrossMultiDatabase() {
    mockConfigurations();
    mockRootMetaData();

    var oldDatabaseName = "db1";
    var newDatabaseName = "db2";
    var oldTableName = "oldTable";
    var newTableName = "newTable";
    var context = buildContext();
    var command =
        new RenameTableCommand(DATASOURCE, oldDatabaseName, newDatabaseName, oldTableName, newTableName);

    var table = prepareTable(oldTableName);
    var oldDatabase = prepareDatabase(oldDatabaseName, table);
    var newDatabase = prepareDatabase(newDatabaseName);

    when(rootMetaData.getDatabase(DATASOURCE, oldDatabaseName)).thenReturn(oldDatabase);
    when(rootMetaData.getDatabase(DATASOURCE, newDatabaseName)).thenReturn(newDatabase);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var databaseMetaData = bytesToDatabaseMetaData((byte[]) args[1]);
              if (databaseMetaData.getName().equals(oldDatabaseName)) {
                assertFalse(databaseMetaData.hasTable(oldTableName));
                assertFalse(databaseMetaData.hasTable(newTableName));
                assertEquals(getPersistPath(oldDatabase.getIdentity()), path);
              } else {
                assertEquals(newDatabaseName, databaseMetaData.getName());
                var newTable = databaseMetaData.getTable(newTableName);
                assertFalse(databaseMetaData.hasTable(oldTableName));
                assertEquals(getPersistPath(newDatabase.getIdentity()), path);
                assertEquals(table.getIdentity(), newTable.getIdentity());
                assertEquals(newTableName, newTable.getName());
                assertEquals(2, newTable.getNumberOfColumns());
              }
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository, times(2)).save(anyString(), any(byte[].class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableNameUnChange() {
    mockConfigurations();
    var databaseName = "db";
    var tableName = "table";
    var context = buildContext();
    var command =
        new RenameTableCommand(DATASOURCE, databaseName, databaseName, tableName, tableName);
    command.execute(context);
  }
}
