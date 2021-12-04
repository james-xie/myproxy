package com.gllue.transport.backend.datasource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import com.gllue.common.concurrent.PlainFuture;
import com.gllue.transport.backend.connection.BackendConnection;
import com.gllue.transport.backend.connection.BackendConnectionFactory;
import com.gllue.transport.backend.connection.BackendConnectionImpl;
import com.gllue.transport.backend.connection.ConnectionArguments;
import com.gllue.transport.backend.BackendConnectionException;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackendDataSourceTest {
  static final String DATA_SOURCE_NAME = "test data source";

  @Mock BackendConnectionFactory factory;

  @Mock ConnectionArguments connectionArguments;

  @Test
  public void testConnectionAcquireAndRelease() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    var future = new PlainFuture<BackendConnection>();
    future.set(prepareConnection(dataSource));
    Mockito.when(factory.newConnection(any())).thenReturn(future);

    var connection = dataSource.acquireConnection("db1");
    assertTrue(connection.isAssigned());
    assertEquals(1, dataSource.acquiredConnections());
    assertEquals(0, dataSource.cachedConnections());
    assertEquals(connection.dataSource(), dataSource);
    assertTrue(connection.release());
    assertTrue(connection.isReleased());
    assertEquals(0, dataSource.acquiredConnections());
    assertEquals(1, dataSource.cachedConnections());

    var connection1 = dataSource.acquireConnection("db1");
    assertEquals(connection, connection1);
    assertTrue(connection1.isAssigned());
  }

  @Test(expected = IllegalStateException.class)
  public void testRepeatReleaseConnection() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    var future = new PlainFuture<BackendConnection>();
    future.set(prepareConnection(dataSource));
    Mockito.when(factory.newConnection(any())).thenReturn(future);

    var connection = dataSource.acquireConnection("db1");
    dataSource.releaseConnection(connection);
    dataSource.releaseConnection(connection);
  }

  @Test
  public void testCloseConnectionAfterConnectionReleased() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    var future = new PlainFuture<BackendConnection>();
    future.set(prepareConnection(dataSource));
    Mockito.when(factory.newConnection(any())).thenReturn(future);

    var connection = dataSource.acquireConnection("db1");
    connection.release();
    connection.close();
    assertEquals(0, dataSource.acquiredConnections());
    assertEquals(0, dataSource.cachedConnections());
  }

  @Test
  public void testCloseConnectionAfterConnectionAssigned() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    var future = new PlainFuture<BackendConnection>();
    future.set(prepareConnection(dataSource));
    Mockito.when(factory.newConnection(any())).thenReturn(future);

    var connection = dataSource.acquireConnection("db1");
    connection.close();
    assertEquals(0, dataSource.acquiredConnections());
    assertEquals(0, dataSource.cachedConnections());
  }

  @Test
  public void testRepeatCloseConnection() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    var future = new PlainFuture<BackendConnection>();
    future.set(prepareConnection(dataSource));
    Mockito.when(factory.newConnection(any())).thenReturn(future);

    var connection = dataSource.acquireConnection("db1");
    connection.close();
    connection.close();
    connection.close();
    assertEquals(0, dataSource.acquiredConnections());
    assertEquals(0, dataSource.cachedConnections());
  }

  @Test
  public void testMultiAcquireAndRelease() {
    var dataSource = prepareDataSource(10);
    assertEquals(0, dataSource.acquiredConnections());

    Mockito.when(factory.newConnection(any()))
        .thenAnswer(
            invocation -> {
              var future = new PlainFuture<BackendConnection>();
              future.set(prepareConnection(dataSource));
              return future;
            });

    for (int i = 0; i < 10; i++) {
      var connection = dataSource.acquireConnection("db1");
      connection.release();
    }
    assertEquals(0, dataSource.acquiredConnections());
    assertEquals(1, dataSource.cachedConnections());

    var connections = new BackendConnection[10];
    for (int i = 0; i < 10; i++) {
      connections[i] = dataSource.acquireConnection("db1");
    }
    assertEquals(10, dataSource.acquiredConnections());
    assertEquals(0, dataSource.cachedConnections());
    for (int i = 0; i < 5; i++) {
      connections[i].release();
    }
    assertEquals(5, dataSource.acquiredConnections());
    assertEquals(5, dataSource.cachedConnections());
  }

  @Test(expected = BackendConnectionException.class)
  public void testExceedMaxCapacity() {
    var maxCapacity = 10;
    var dataSource = prepareDataSource(maxCapacity);
    assertEquals(0, dataSource.acquiredConnections());

    Mockito.when(factory.newConnection(any()))
        .thenAnswer(
            invocation -> {
              var future = new PlainFuture<BackendConnection>();
              future.set(prepareConnection(dataSource));
              return future;
            });

    for (int i = 0; i <= maxCapacity; i++) {
      dataSource.acquireConnection("db1");
    }
  }

  @Test
  public void testConcurrentAcquire() throws Exception {
    var maxCapacity = 1000;
    var dataSource = prepareDataSource(maxCapacity);
    assertEquals(0, dataSource.acquiredConnections());

    Mockito.when(factory.newConnection(any()))
        .thenAnswer(
            invocation -> {
              var future = new PlainFuture<BackendConnection>();
              future.set(prepareConnection(dataSource));
              return future;
            });

    var acquiredCount = new AtomicInteger(0);

    var halfCapacity = maxCapacity / 2;
    var latch1 = new CountDownLatch(halfCapacity);
    var threadPool = Executors.newFixedThreadPool(10);
    var connections = new ConcurrentLinkedQueue<BackendConnection>();
    for (int i = 0; i < halfCapacity; i++) {
      threadPool.submit(
          () -> {
            var conn = dataSource.acquireConnection("db1");
            connections.add(conn);
            acquiredCount.incrementAndGet();
            latch1.countDown();
          });
    }
    latch1.await();

    var latch2 = new CountDownLatch(halfCapacity);
    for (int i = 0; i < halfCapacity; i++) {
      threadPool.submit(
          () -> {
            var random = ThreadLocalRandom.current();
            if (random.nextBoolean()) {
              var conn = dataSource.acquireConnection("db1");
              connections.add(conn);
              acquiredCount.incrementAndGet();
            } else {
              var conn = connections.poll();
              assert conn != null;
              conn.release();
            }
            latch2.countDown();
          });
    }

    latch2.await();

    threadPool.shutdown();

    int releasedCount = maxCapacity - acquiredCount.get();
    assertEquals(acquiredCount.get() - releasedCount, dataSource.acquiredConnections());
  }

  private BackendDataSource prepareDataSource(final int capacity) {
    return new BackendDataSource(DATA_SOURCE_NAME, capacity, factory, connectionArguments);
  }

  private BackendConnection prepareConnection(final BackendDataSource dataSource) {
    EmbeddedChannel ch = new EmbeddedChannel();
    var connection = new BackendConnectionImpl(1, ch);
    connection.setDataSource(dataSource);
    return connection;
  }
}
