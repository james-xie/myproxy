package com.gllue.myproxy.metadata.command;

import static org.junit.Assert.assertEquals;
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
public class CreateDatabaseCommandTest extends BaseCommandTest {
  static final String DATASOURCE = "ds";

  @Test
  public void testAddDatabase() {
    mockConfigurations();
    mockRootMetaData();

    var databaseName = "db";
    var context = buildContext();
    var command = new CreateDatabaseCommand(DATASOURCE, databaseName);

    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              var newDb = bytesToDatabaseMetaData((byte[]) args[1]);
              assertEquals(databaseName, newDb.getName());
              return null;
            })
        .when(repository)
        .save(anyString(), any());

    command.execute(context);

    verify(repository).save(eq(getPersistPath(dbKey(databaseName))), any(byte[].class));
    verify(rootMetaData).addDatabase(any(), eq(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddExistsDatabase() {
    mockConfigurations();

    var databaseName = "db";
    var context = buildContext();
    var command = new CreateDatabaseCommand(DATASOURCE, databaseName);
    when(rootMetaData.hasDatabase(DATASOURCE, databaseName)).thenReturn(true);
    command.execute(context);
  }
}
