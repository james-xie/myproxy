package com.gllue.metadata.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropTableCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testDropTable() {
    mockConfigurations();

    var databaseName = "db";
    var tableName = "table";
    var context = buildContext();
    var command = new DropTableCommand(DATASOURCE, databaseName, tableName);

    var database = prepareDatabase(databaseName);
    var table = prepareTable(tableName);
    database.addTable(table);
    when(rootMetaData.getDatabase(DATASOURCE, databaseName)).thenReturn(database);

    command.execute(context);

    verify(repository).delete(eq(getPersistPath(database.getIdentity(), table.getIdentity())));
  }
}
