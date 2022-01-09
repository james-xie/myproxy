package com.gllue.myproxy.command.handler.query.dcl.set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.BadEncryptKeyException;
import com.gllue.myproxy.command.handler.query.BaseQueryHandlerTest;
import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.FuturableCallback;
import com.gllue.myproxy.common.Promise;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class SetStatementHandlerTest extends BaseQueryHandlerTest {

  public SetStatementHandler newHandler() {
    return new SetStatementHandler(transportService, threadPool);
  }

  @Test
  public void testSetEncryptKey() throws ExecutionException, InterruptedException {
    var query = String.format("set encrypt_key='%s:%s'", DATABASE, ENCRYPT_KEY);
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callbackGet(callback);

    verify(sessionContext).setEncryptKey(eq(ENCRYPT_KEY));
  }

  @Test(expected = BadEncryptKeyException.class)
  public void testSetMalformedEncryptKey() {
    var query = String.format("set encrypt_key='%s:'", DATABASE);
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);
  }

  @Test(expected = BadEncryptKeyException.class)
  public void testSetEncryptKeyWithBadDatabase() {
    var query = String.format("set encrypt_key='%s:%s'", "bad_db", ENCRYPT_KEY);
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);
  }

  @Test
  public void testSetAutoCommit() throws ExecutionException, InterruptedException {
    when(transportService.setAutoCommit(anyInt(), anyBoolean()))
        .thenReturn(Promise.emptyPromise(CommandResult.newEmptyResult()));

    var query = "set autocommit=ON";
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callbackGet(callback);

    verify(transportService).setAutoCommit(eq(request.getConnectionId()), eq(true));
  }

  @Test
  public void testSetGlobalAutoCommit() throws ExecutionException, InterruptedException {
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);

    var query = "set global autocommit=off";
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callbackGet(callback);

    verify(transportService, times(0)).setAutoCommit(eq(request.getConnectionId()), anyBoolean());
    assertEquals(1, submitSqlList.size());
    assertSQLEquals(query, submitSqlList.get(0));
  }

  @Test(expected = BadVariableValueException.class)
  public void testSetAutoCommitWithWrongValue() throws ExecutionException, InterruptedException {
    var query = "set autocommit=GOOD";
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callbackGet(callback);
  }

  @Test
  public void testMixedSet() throws ExecutionException, InterruptedException {
    var submitSqlList = new ArrayList<String>();
    mockTransportService(submitSqlList);
    when(transportService.setAutoCommit(anyInt(), anyBoolean()))
        .thenReturn(Promise.emptyPromise(CommandResult.newEmptyResult()));

    var query = String.format("set autocommit=off, encrypt_key='%s:%s', @a=1", DATABASE, ENCRYPT_KEY);
    var request = newQueryHandlerRequest(query, Map.of());
    var callback = new FuturableCallback<HandlerResult>();

    var handler = newHandler();
    handler.execute(request, callback);

    callbackGet(callback);

    verify(sessionContext).setEncryptKey(eq(ENCRYPT_KEY));
    verify(transportService).setAutoCommit(eq(request.getConnectionId()), eq(false));
    assertEquals(1, submitSqlList.size());
    assertSQLEquals("set @a=1", submitSqlList.get(0));
  }
}
