package com.gllue.myproxy.command.handler.query.dml.select;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequestImpl;
import com.gllue.myproxy.common.FuturableCallback;
import com.gllue.myproxy.common.exception.NoDatabaseException;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectQueryHandlerTest extends BaseQueryHandlerTest {
  SelectQueryHandler newHandler() {
    return new SelectQueryHandler(
        repository, configurations, clusterState, transportService, threadPool);
  }

  @Before
  public void setUp() {
    mockThreadPool();
  }

  @Test
  public void testExecuteSimpleSelectQuery() throws ExecutionException, InterruptedException {
    var submitSqlList = new ArrayList<String>();
    mockSubmitQueryAndDirectTransferResult(submitSqlList);

    var handler = newHandler();
    var queries =
        new String[] {"select sleep(1)", "select version()", "select 1+1+2", "select database()"};
    for (var query : queries) {
      var request = newQueryHandlerRequest(query, Map.of());
      var callback = new FuturableCallback<HandlerResult>();
      handler.execute(request, callback);
      callback.get();
    }

    assertEquals(queries.length, submitSqlList.size());
    for (int i = 0; i < queries.length; i++) {
      assertSQLEquals(queries[i], submitSqlList.get(i));
    }
  }

  protected void mockConfigurations() {
    when(configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.ENCRYPTION_ALGORITHM))
        .thenReturn("AES");
  }

  @Test(expected = NoDatabaseException.class)
  public void testExecuteQueryNoDatabase() throws ExecutionException, InterruptedException {
    var submitSqlList = new ArrayList<String>();
    mockSubmitQueryAndDirectTransferResult(submitSqlList);
    mockClusterState(prepareMultiDatabasesMetaData(null));
    mockConfigurations();

    var query = "select * from table1";

    var handler = newHandler();
    var request =
        new QueryHandlerRequestImpl(
            FRONTEND_CONNECTION_ID, DATASOURCE, null, query, sessionContext);
    request.setStatement(sqlParser.parse(query));
    var callback = new FuturableCallback<HandlerResult>();
    handler.execute(request, callback);
    callback.get();
  }

  @Test
  public void testExecuteQueryWithRewrite() throws ExecutionException, InterruptedException {
    var table1 = prepareTable("table1", "id", "col1", "col2", "col3");

    var submitSqlList = new ArrayList<String>();
    mockSubmitQueryAndDirectTransferResult(submitSqlList);
    mockClusterState(prepareMultiDatabasesMetaData(table1));
    mockConfigurations();

    var query = "select * from table1";

    var handler = newHandler();
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();
    handler.execute(request, callback);
    callback.get();

    assertEquals(1, submitSqlList.size());
    assertSQLEquals(
        "SELECT table1.`id`, table1.`col1`, table1.`col2`, table1.`col3` FROM table1",
        submitSqlList.get(0));
  }

  @Test
  public void testExecuteQueryWithoutWrite() throws ExecutionException, InterruptedException {
    var table1 = prepareTable("table1", "id", "col1", "col2", "col3");

    var submitSqlList = new ArrayList<String>();
    mockSubmitQueryAndDirectTransferResult(submitSqlList);
    mockClusterState(prepareMultiDatabasesMetaData(table1));
    mockConfigurations();

    var query = "select id, col1, col2 from table1";

    var handler = newHandler();
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();
    handler.execute(request, callback);
    callback.get();

    assertEquals(1, submitSqlList.size());
    assertSQLEquals(query, submitSqlList.get(0));
  }
}
