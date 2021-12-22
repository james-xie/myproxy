package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.metadata.model.ColumnType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UpdateColumnCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testUpdateColumnProperties() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var columnName = "col1";
    var type = ColumnType.BLOB;
    var nullable = false;
    var defaultValue = "default";
    var context = buildContext();
    var command =
        new UpdateColumnCommand(
            DATASOURCE,
            databaseName,
            tableName,
            columnName,
            columnName,
            type,
            nullable,
            defaultValue);

    var table = prepareTable(tableName);
    var database = prepareDatabase(databaseName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var newTable = bytesToTableMetaData((byte[]) args[1]);
              assertEquals(tableName, newTable.getName());
              assertEquals(2, newTable.getNumberOfColumns());

              var newColumn = newTable.getColumn(columnName);
              assertNotNull(newColumn);
              assertEquals(columnName, newColumn.getName());
              assertEquals(type, newColumn.getType());
              assertEquals(nullable, newColumn.isNullable());
              assertEquals(defaultValue, newColumn.getDefaultValue());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository)
        .save(eq(getPersistPath(database.getIdentity(), table.getIdentity())), any(byte[].class));
  }

  @Test
  public void testRenameColumn() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var oldColumnName = "col1";
    var newColumnName = "col_1";
    var type = ColumnType.VARCHAR;
    var nullable = false;
    var context = buildContext();
    var command =
        new UpdateColumnCommand(
            DATASOURCE,
            databaseName,
            tableName,
            oldColumnName,
            newColumnName,
            type,
            nullable,
            null);

    var table = prepareTable(tableName);
    var database = prepareDatabase(databaseName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var newTable = bytesToTableMetaData((byte[]) args[1]);
              assertEquals(tableName, newTable.getName());
              assertEquals(2, newTable.getNumberOfColumns());

              assertNull(newTable.getColumn(oldColumnName));
              var newColumn = newTable.getColumn(newColumnName);
              assertNotNull(newColumn);
              assertEquals(newColumnName, newColumn.getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository)
        .save(eq(getPersistPath(database.getIdentity(), table.getIdentity())), any(byte[].class));
  }
}
