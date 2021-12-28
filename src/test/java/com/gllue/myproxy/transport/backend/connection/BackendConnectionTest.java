package com.gllue.myproxy.transport.backend.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackendConnectionTest {

  @Mock
  BackendDataSource dataSource;

  @Test
  public void testConnectionAssignAndRelease() {
    var connection = prepareConnection();
    connection.assign();
    assertTrue(connection.release());
    assertFalse(connection.release());
  }

  @Test(expected = AssertionError.class)
  public void testConnectionAssignTwice() {
    var connection = prepareConnection();
    connection.assign();
    connection.assign();
  }

  @Test(expected = IllegalStateException.class)
  public void testConnectionAssignWhenConnectionClosed() {
    var connection = prepareConnection();
    connection.close();
    assertTrue(connection.isClosed());
    connection.assign();
  }

  @Test
  public void testConnectionReleaseWhenConnectionClosed() {
    var connection = prepareConnection();
    connection.assign();
    connection.close();
    assertTrue(connection.isClosed());
    assertFalse(connection.release());
  }


  private BackendConnection prepareConnection() {
    EmbeddedChannel ch = new EmbeddedChannel();
    var connection = new BackendConnectionImpl(1, ch);
    connection.setDataSource(dataSource);
    return connection;
  }
}
