package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.metadata.command.AbstractTableUpdateCommand.Column;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.TableType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreatePartitionTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testCreatePartitionTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var columnName1 = "col1";
    var columnName2 = "col2";
    var columnName3 = "col3";
    var columnName4 = "col4";
    var columnName5 = "col5";
    var context = buildContext();
    var primaryTable =
        new CreatePartitionTableCommand.Table(
            tableName,
            new Column[] {
              new Column(columnName1, ColumnType.INT, false, null),
              new Column(columnName2, ColumnType.VARCHAR, false, null),
            });

    var extensionTables =
        new CreatePartitionTableCommand.Table[] {
          new CreatePartitionTableCommand.Table(
              tableName + "-1",
              new Column[] {
                new Column(columnName3, ColumnType.DATE, false, null),
                new Column(columnName4, ColumnType.INT, false, null),
              }),
          new CreatePartitionTableCommand.Table(
              tableName + "-2",
              new Column[] {
                new Column(columnName5, ColumnType.VARCHAR, false, null),
              })
        };

    var command =
        new CreatePartitionTableCommand(
            DATASOURCE, databaseName, tableName, primaryTable, extensionTables);

    var database = prepareDatabase(databaseName);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newTable = bytesToPartitionTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
              Assert.assertEquals(tableName, newTable.getName());
              Assert.assertEquals(TableType.PARTITION, newTable.getType());
              Assert.assertEquals(5, newTable.getNumberOfColumns());
              Assert.assertEquals(2, newTable.getPrimaryTable().getNumberOfColumns());
              Assert.assertEquals(2, newTable.getNumberOfExtensionTables());
              Assert.assertEquals("col1", newTable.getColumn(0).getName());
              Assert.assertEquals("col5", newTable.getColumn(4).getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
  }
}
