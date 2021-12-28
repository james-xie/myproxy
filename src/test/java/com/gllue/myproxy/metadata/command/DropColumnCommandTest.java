package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropColumnCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testDropColumn() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var columnName = "col1";
    var context = buildContext();
    var command = new DropColumnCommand(DATASOURCE, databaseName, tableName, columnName);

    var table = prepareTable(tableName);
    var database = prepareDatabase(databaseName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var newTable = bytesToTableMetaData((byte[]) args[1]);
              assertEquals(tableName, newTable.getName());
              assertEquals(1, newTable.getNumberOfColumns());
              assertFalse(newTable.hasColumn(columnName));
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository)
        .save(eq(getPersistPath(database.getIdentity(), table.getIdentity())), any(byte[].class));
  }
}
