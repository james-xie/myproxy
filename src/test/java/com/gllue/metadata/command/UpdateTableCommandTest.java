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
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpdateTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testUpdateTable() {
    mockConfigurations();

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
              var newTable = bytesToTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
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
  }

  @Test
  public void testUpdatePartitionTableToStandardTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var identity = RandomUtils.randomShortUUID();

    var database = prepareDatabase(databaseName);
    var partitionTableBuilder =
        new PartitionTableMetaData.Builder()
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
          var newTable = bytesToTableMetaData((byte[]) args[1]);

          assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
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
  }
}
