package com.gllue.myproxy.metadata.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DropDatabaseCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testDropDatabase() {
    mockConfigurations();

    var databaseName = "db";
    var context = buildContext();
    var command = new DropDatabaseCommand(DATASOURCE, databaseName);

    when(rootMetaData.getDatabase(DATASOURCE, databaseName))
        .thenReturn(prepareDatabase(databaseName));

    command.execute(context);

    verify(repository).delete(eq(getPersistPath(dbKey(databaseName))));
  }
}
