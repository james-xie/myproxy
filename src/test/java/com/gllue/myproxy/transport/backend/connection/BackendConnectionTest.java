package com.gllue.myproxy.transport.backend.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackendConnectionTest {
  QueryCommandPacket commandPacket = new QueryCommandPacket("select 1");

  @Test
  public void testSendCommand() {
    var connection = prepareConnection();
    var future = connection.sendCommand(commandPacket);
    assertNotNull(connection.getCommandResultReader());
    connection.onResponseReceived();
    connection.setCommandExecutionDone();
    assertTrue(future.isDone());
  }

  @Test(expected = IllegalStateException.class)
  public void testSendCommandConflict() {
    var connection = prepareConnection();
    connection.sendCommand(commandPacket);
    connection.sendCommand(commandPacket);
  }

  @Test
  public void testConnectionClose() {
    var connection = prepareConnection();
    connection.close();
    assertTrue(connection.isClosed());
  }

  private BackendConnection prepareConnection() {
    EmbeddedChannel ch = new EmbeddedChannel();
    var connection = new BackendConnectionImpl(1, "root", ch, 1);
    connection.setDataSourceName("datasource");
    return connection;
  }
}
