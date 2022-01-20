package com.gllue.myproxy.transport.core.connection;

import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_IDLE_TIMEOUT_IN_MILLS;
import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_KEEP_ALIVE_TIME_IN_MILLS;
import static com.gllue.myproxy.transport.core.connection.AbstractConnectionPool.DEFAULT_MAX_LIFE_TIME_IN_MILLS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.concurrent.DoneFuture;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FIFOConnectionPoolTest {
  @Mock ConnectionArguments arguments;
  @Mock ConnectionFactory factory;
  @Mock ScheduledThreadPoolExecutor scheduler;
  ExecutorService executor = ThreadPool.DIRECT_EXECUTOR_SERVICE;

  ConnectionPool newConnectionPool(int maxPoolSize) {
    return new FIFOConnectionPool(arguments, factory, maxPoolSize, scheduler, executor);
  }

  Connection newMockConnection(String dbName) {
    var currentTime = System.currentTimeMillis();
    return newMockConnection(dbName, currentTime);
  }

  Connection newMockConnection(String dbName, long currentTime) {
    var conn = mock(Connection.class);
    when(conn.createTime()).thenReturn(currentTime);
    when(conn.lastAccessTime()).thenReturn(currentTime);
    when(conn.currentDatabase()).thenReturn(dbName);
    return conn;
  }

  void mockFactory() {
    when(factory.newConnection(any(), eq(null)))
        .thenAnswer(
            (invocation -> {
              var conn = newMockConnection(null);
              return new DoneFuture<>(conn);
            }));
  }

  @Test
  public void testAcquireAndReleaseConnection() {
    mockFactory();

    int poolSize = 100;
    var pool = newConnectionPool(poolSize);
    var connections = new Connection[poolSize];
    for (int i = 0; i < poolSize; i++) {
      var conn = pool.acquireConnection(null);
      assertTrue(conn instanceof PooledConnection);
      connections[i] = conn;
    }
    verify(factory, times(poolSize)).newConnection(any(), eq(null));

    int halfPoolSize = poolSize / 2;
    for (int i = 0; i < halfPoolSize; i++) {
      pool.releaseConnection(connections[i]);
    }

    for (int i = 0; i < halfPoolSize; i++) {
      var conn = pool.acquireConnection(null);
      assertTrue(conn instanceof PooledConnection);
      connections[i] = conn;
    }
    verify(factory, times(poolSize)).newConnection(any(), eq(null));
    assertEquals(poolSize, Arrays.stream(connections).collect(Collectors.toSet()).size());

    for (int i = 0; i < poolSize; i++) {
      pool.releaseConnection(connections[i]);
    }
    assertEquals(0, pool.getNumberOfAcquiredConnections());
    assertEquals(poolSize, pool.getNumberOfCachedConnections());
  }

  @Test
  public void testConcurrentAcquireAndReleaseConnection() throws InterruptedException {
    mockFactory();

    var pool = newConnectionPool(1000);
    var executor = Executors.newFixedThreadPool(10);
    var latch = new CountDownLatch(5);
    var queue = new LinkedBlockingQueue<Connection>();
    for (int i = 0; i < 5; i++) {
      executor.execute(
          () -> {
            for (int j = 0; j < 200; j++) {
              queue.add(pool.acquireConnection(null));
            }
          });
      executor.execute(
          () -> {
            for (int j = 0; j < 200; j++) {
              try {
                pool.releaseConnection(queue.take());
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            latch.countDown();
          });
    }
    latch.await();
    executor.shutdown();
    assertEquals(0, pool.getNumberOfAcquiredConnections());
  }

  @Test
  public void testAcquireConnectionWithDatabase() {
    var dbNameCount = new AtomicInteger();
    when(factory.newConnection(any(), nullable(String.class)))
        .thenAnswer(
            (invocation -> {
              var conn = newMockConnection("db" + dbNameCount.incrementAndGet());
              return new DoneFuture<>(conn);
            }));

    var pool = newConnectionPool(10);
    var connections = new Connection[10];
    var dbNames = new ArrayList<String>();
    for (int i = 0; i < connections.length; i++) {
      connections[i] = pool.acquireConnection(null);
      var dbName = connections[i].currentDatabase();
      assertNotNull(dbName);
      dbNames.add(dbName);
    }

    for (Connection connection : connections) {
      pool.releaseConnection(connection);
    }

    var random = new Random();
    for (int i = connections.length - 1; i >= 0; i--) {
      var index = random.nextInt(dbNames.size());
      var dbName = dbNames.get(index);
      var conn = pool.acquireConnection(dbName);
      assertEquals(dbName, conn.currentDatabase());
      dbNames.remove(index);
    }
  }

  @Test(expected = TooManyConnectionsException.class)
  public void testConnectionPoolCapacity() {
    mockFactory();
    var pool = newConnectionPool(1);
    pool.acquireConnection(null);
    pool.acquireConnection(null);
  }

  @Test
  public void testRemoveIdleConnection() {
    var idleConnectionCount = 2;
    var idleConnectionCounter = new AtomicInteger(idleConnectionCount);

    when(factory.newConnection(any(), eq(null)))
        .thenAnswer(
            (invocation -> {
              Connection connection;
              if (idleConnectionCounter.getAndDecrement() > 0) {
                var currentTime = System.currentTimeMillis() - DEFAULT_IDLE_TIMEOUT_IN_MILLS;
                connection = newMockConnection(null, currentTime - 1);
              } else {
                connection = newMockConnection(null);
              }
              return new DoneFuture<>(connection);
            }));

    var pool = newConnectionPool(5);
    var connections = new Connection[pool.getMaxPoolSize()];
    for (int i = 0; i < connections.length; i++) {
      connections[i] = pool.acquireConnection(null);
    }
    for (var conn : connections) {
      pool.releaseConnection(conn);
    }

    assertEquals(connections.length, pool.getNumberOfCachedConnections());

    ArgumentCaptor<Runnable> argCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler).scheduleWithFixedDelay(argCaptor.capture(), anyLong(), anyLong(), any());
    var runnable = argCaptor.getValue();
    runnable.run();

    assertEquals(connections.length - idleConnectionCount, pool.getNumberOfCachedConnections());

    for (int i = 0; i < connections.length; i++) {
      connections[i] = pool.acquireConnection(null);
    }
    runnable.run();

    for (var pooledConn : connections) {
      var conn = ((PooledConnection) pooledConn).getPoolEntry().getConnection();
      verify(conn, times(0)).close();
      pool.releaseConnection(pooledConn);
    }
    assertEquals(connections.length, pool.getNumberOfCachedConnections());
  }

  @Test
  public void testConnectionKeepAliveForAlwaysAliveConnection() {
    mockFactory();

    var pool = newConnectionPool(5);
    var pooledConn = pool.acquireConnection(null);
    pool.releaseConnection(pooledConn);

    ArgumentCaptor<Runnable> argCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(2)).schedule(argCaptor.capture(), anyLong(), any());
    argCaptor.getAllValues().get(0).run();

    var connection = ((PooledConnection) pooledConn).getPoolEntry().getConnection();
    verify(connection, times(0)).writeAndFlush(any(), any());
  }

  @Test
  public void testConnectionKeepAlive() {
    when(factory.newConnection(any(), eq(null)))
        .thenAnswer(
            (invocation -> {
              var currentTime = System.currentTimeMillis() - DEFAULT_KEEP_ALIVE_TIME_IN_MILLS;

              var conn = mock(BackendConnection.class);
              when(conn.createTime()).thenReturn(currentTime);
              when(conn.lastAccessTime()).thenReturn(currentTime);
              when(conn.sendCommand(any()))
                  .thenReturn(new DoneFuture<>(CommandResult.newEmptyResult()));
              return new DoneFuture<>(conn);
            }));

    var pool = newConnectionPool(5);
    var pooledConn = pool.acquireConnection(null);
    pool.releaseConnection(pooledConn);

    ArgumentCaptor<Runnable> argCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(2)).schedule(argCaptor.capture(), anyLong(), any());
    argCaptor.getAllValues().get(0).run();

    var connection =
        (BackendConnection) ((PooledConnection) pooledConn).getPoolEntry().getConnection();
    verify(connection, times(1)).sendCommand(any());
  }

  @Test
  public void testKeepAliveTimeout() {
    var keepAliveQueryTimeoutInMills = 100;
    when(factory.newConnection(any(), eq(null)))
        .thenAnswer(
            (invocation -> {
              var currentTime = System.currentTimeMillis() - DEFAULT_KEEP_ALIVE_TIME_IN_MILLS;
              var conn = mock(BackendConnection.class);
              when(conn.createTime()).thenReturn(currentTime);
              when(conn.lastAccessTime()).thenReturn(currentTime);
              when(conn.sendCommand(any())).thenReturn(new PlainFuture<>());
              return new DoneFuture<>(conn);
            }));

    var pool =
        new FIFOConnectionPool(
            arguments,
            factory,
            5,
            DEFAULT_IDLE_TIMEOUT_IN_MILLS,
            DEFAULT_KEEP_ALIVE_TIME_IN_MILLS,
            keepAliveQueryTimeoutInMills,
            DEFAULT_MAX_LIFE_TIME_IN_MILLS,
            scheduler,
            executor);
    var pooledConn = pool.acquireConnection(null);
    pool.releaseConnection(pooledConn);

    ArgumentCaptor<Runnable> argCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(2)).schedule(argCaptor.capture(), anyLong(), any());
    argCaptor.getAllValues().get(0).run();

    var connection =
        (BackendConnection) ((PooledConnection) pooledConn).getPoolEntry().getConnection();
    verify(connection, times(1)).sendCommand(any());
    verify(connection, times(1)).close();
  }

  @Test
  public void testConnectionMaxLifeTime() {
    when(factory.newConnection(any(), eq(null)))
        .thenAnswer(
            (invocation -> {
              var currentTime = System.currentTimeMillis() - DEFAULT_MAX_LIFE_TIME_IN_MILLS;
              return new DoneFuture<>(newMockConnection(null, currentTime - 1));
            }));

    var pool = newConnectionPool(5);
    var pooledConn = pool.acquireConnection(null);

    ArgumentCaptor<Runnable> argCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduler, times(2)).schedule(argCaptor.capture(), anyLong(), any());
    var runnable = argCaptor.getAllValues().get(1);
    runnable.run();

    var connection = ((PooledConnection) pooledConn).getPoolEntry().getConnection();
    verify(connection, times(0)).close();

    pool.releaseConnection(pooledConn);
    runnable.run();

    verify(connection, times(1)).close();
  }
}
