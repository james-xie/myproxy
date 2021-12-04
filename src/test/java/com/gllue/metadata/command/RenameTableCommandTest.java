package com.gllue.metadata.command;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RenameTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testRenameTable() {
    mockConfigurations();

    var databaseName = "db";
    var oldTableName = "oldTable";
    var newTableName = "newTable";
    var context = buildContext();
    var command = new RenameTableCommand(DATASOURCE, databaseName, oldTableName, newTableName);

    var database = prepareDatabase(databaseName);
    var table = prepareTable(oldTableName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newTable = bytesToTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), table.getIdentity()), path);
              assertEquals(table.getIdentity(), newTable.getIdentity());
              assertEquals(newTableName, newTable.getName());
              assertEquals(2, newTable.getNumberOfColumns());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableNameUnChange() {
    mockConfigurations();
    var databaseName = "db";
    var tableName = "table";
    var context = buildContext();
    var command = new RenameTableCommand(DATASOURCE, databaseName, tableName, tableName);
    command.execute(context);
  }
}
