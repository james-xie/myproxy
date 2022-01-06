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
public class DropTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testDropTable() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var tableName = "table";
    var context = buildContext();
    var command = new DropTableCommand(DATASOURCE, databaseName, tableName);

    var database = prepareDatabase(databaseName);
    var table = prepareTable(tableName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var path = (String) args[0];
              var newDatabase = bytesToDatabaseMetaData((byte[]) args[1]);
              assertEquals(getPersistPath(database.getIdentity()), path);
              assertFalse(newDatabase.hasTable(tableName));
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(anyString(), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }
}
