package com.gllue.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.common.util.RandomUtils;
import com.gllue.metadata.command.AbstractTableUpdateCommand.Column;
import com.gllue.metadata.model.ColumnType;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpdatePartitionTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testUpdatePartitionTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var identity = RandomUtils.randomShortUUID();
    var columnName1 = "col1";
    var columnName2 = "col2";
    var columnName3 = "col3";
    var columnName4 = "col4";
    var columnName5 = "col5";
    var context = buildContext();
    var primaryTable =
        new UpdatePartitionTableCommand.Table(
            tableName,
            new Column[] {
              new Column(columnName1, ColumnType.INT, false, null),
              new Column(columnName2, ColumnType.VARCHAR, false, null),
            });

    var extensionTables =
        new UpdatePartitionTableCommand.Table[] {
          new UpdatePartitionTableCommand.Table(
              tableName + "-1",
              new Column[] {
                new Column(columnName3, ColumnType.DATE, false, null),
                new Column(columnName4, ColumnType.INT, false, null),
              }),
          new UpdatePartitionTableCommand.Table(
              tableName + "-2",
              new Column[] {
                new Column(columnName5, ColumnType.VARCHAR, false, null),
              })
        };

    var command =
        new UpdatePartitionTableCommand(
            DATASOURCE, databaseName, identity, tableName, primaryTable, extensionTables);

    var database = prepareDatabase(databaseName);
    var partitionTableBuilder =
        new PartitionTableMetaData.Builder()
            .setName(tableName)
            .setIdentity(identity)
            .setPrimaryTable(prepareTable(tableName));
    var partitionTable = partitionTableBuilder.build();
    database.addTable(partitionTable);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newTable = bytesToPartitionTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
              assertEquals(tableName, newTable.getName());
              assertEquals(TableType.PARTITION, newTable.getType());
              assertEquals(5, newTable.getNumberOfColumns());
              assertEquals(2, newTable.getPrimaryTable().getNumberOfColumns());
              assertEquals(2, newTable.getNumberOfExtensionTables());
              assertEquals("col1", newTable.getColumn(0).getName());
              assertEquals("col5", newTable.getColumn(4).getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
  }

  @Test
  public void testUpdateStandardTableToPartitionTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";

    var database = prepareDatabase(databaseName);
    var standardTable = prepareTable(tableName);
    database.addTable(standardTable);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    var identity = standardTable.getIdentity();
    var columnName1 = "col1";
    var context = buildContext();
    var primaryTable =
        new UpdatePartitionTableCommand.Table(
            tableName,
            new Column[] {
              new Column(columnName1, ColumnType.INT, false, null),
            });

    var command =
        new UpdatePartitionTableCommand(
            DATASOURCE, databaseName, identity, tableName, primaryTable);
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newTable = bytesToPartitionTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
              assertEquals(tableName, newTable.getName());
              assertEquals(TableType.PARTITION, newTable.getType());
              assertEquals(1, newTable.getNumberOfColumns());
              assertEquals(1, newTable.getPrimaryTable().getNumberOfColumns());
              assertEquals(0, newTable.getNumberOfExtensionTables());
              assertEquals("col1", newTable.getColumn(0).getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
  }
}
