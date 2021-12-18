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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CreateTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testCreateNewTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var columnName1 = "col1";
    var columnName2 = "col2";
    var columnName3 = "col3";
    var tableType = TableType.PRIMARY;
    var context = buildContext();
    var command =
        new CreateTableCommand(
            DATASOURCE,
            databaseName,
            tableName,
            tableType,
            new Column[] {
              new Column(columnName1, ColumnType.DATE, false, null),
              new Column(columnName2, ColumnType.DATE, false, null),
              new Column(columnName3, ColumnType.DATE, false, null),
            });

    var database = prepareDatabase(databaseName);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newTable = bytesToTableMetaData((byte[]) args[1]);

              assertEquals(getPersistPath(database.getIdentity(), newTable.getIdentity()), path);
              assertEquals(tableName, newTable.getName());
              assertEquals(3, newTable.getNumberOfColumns());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
  }
}
