package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.metadata.command.AbstractTableUpdateCommand.Column;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData.Builder;
import com.gllue.myproxy.metadata.model.TableType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpdateTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testUpdateTable() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var tableName = "table";

    var database = prepareDatabase(databaseName);
    var table = prepareTable(tableName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    var newTableName = "new_table";
    var columnName1 = "new_col1";
    var columnName2 = "new_col2";
    var context = buildContext();
    var command =
        new UpdateTableCommand(
            DATASOURCE,
            databaseName,
            table.getIdentity(),
            newTableName,
            new Column[] {
              new Column(columnName1, ColumnType.VARCHAR, true, null),
              new Column(columnName2, ColumnType.INT, false, null),
            });

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newDatabase = bytesToDatabaseMetaData((byte[]) args[1]);
              var newTable = newDatabase.getTable(newTableName);

              assertNull(newDatabase.getTable(tableName));
              assertEquals(getPersistPath(database.getIdentity()), path);
              assertEquals(newTableName, newTable.getName());
              Assert.assertEquals(TableType.STANDARD, newTable.getType());
              assertEquals(2, newTable.getNumberOfColumns());
              assertEquals(columnName1, newTable.getColumn(0).getName());
              assertEquals(columnName2, newTable.getColumn(1).getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }

  @Test
  public void testUpdatePartitionTableToStandardTable() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var tableName = "table";
    var identity = RandomUtils.randomShortUUID();

    var database = prepareDatabase(databaseName);
    var partitionTableBuilder =
        new Builder()
            .setName(tableName)
            .setIdentity(identity)
            .setPrimaryTable(prepareTable(tableName));
    var partitionTable = partitionTableBuilder.build();
    database.addTable(partitionTable);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    var newTableName = "new_table";
    var columnName1 = "new_col1";
    var columnName2 = "new_col2";
    var context = buildContext();
    var command =
        new UpdateTableCommand(
            DATASOURCE,
            databaseName,
            identity,
            newTableName,
            new Column[] {
                new Column(columnName1, ColumnType.VARCHAR, true, null),
                new Column(columnName2, ColumnType.INT, false, null),
            });

    doAnswer(
        invocation -> {
          Object[] args = invocation.getArguments();
          var path = (String) args[0];
          var newDatabase = bytesToDatabaseMetaData((byte[]) args[1]);
          var newTable = newDatabase.getTable(newTableName);

          assertNull(newDatabase.getTable(tableName));
          assertEquals(getPersistPath(database.getIdentity()), path);
          assertEquals(newTableName, newTable.getName());
          assertEquals(TableType.STANDARD, newTable.getType());
          assertEquals(2, newTable.getNumberOfColumns());
          assertEquals(columnName1, newTable.getColumn(0).getName());
          assertEquals(columnName2, newTable.getColumn(1).getName());
          return null;
        })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }
}
