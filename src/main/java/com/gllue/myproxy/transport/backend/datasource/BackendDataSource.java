package com.gllue.myproxy.transport.backend.datasource;

import com.gllue.myproxy.common.concurrent.DoneFuture;
import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionFactory;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.backend.BackendConnectionException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendDataSource implements DataSource<BackendConnection> {
  private final Queue<BackendConnection> connectionQueue = new ConcurrentLinkedQueue<>();

  private final AtomicInteger acquiredConnections = new AtomicInteger(0);

  private final String name;

  private final int maxCapacity;

  private final BackendConnectionFactory connectionFactory;

  private final ConnectionArguments connectionArguments;

  public BackendDataSource(
      final String name,
      final int maxCapacity,
      final BackendConnectionFactory connectionFactory,
      final ConnectionArguments connectionArguments) {
    this.name = name;
    this.maxCapacity = maxCapacity;
    this.connectionFactory = connectionFactory;
    this.connectionArguments = connectionArguments;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int acquiredConnections() {
    return acquiredConnections.get();
  }

  @Override
  public int cachedConnections() {
    return connectionQueue.size();
  }

  @Override
  public int maxCapacity() {
    return maxCapacity;
  }

  @Override
  public void registerConnection(BackendConnection connection) {
    this.connectionQueue.offer(connection);
  }

  @Override
  public BackendConnection acquireConnection(@Nullable final String database) {
    var future = tryAcquireConnection(database);
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new BackendConnectionException(e, "Failed to acquire new connection.");
    }
  }

  @Override
  public ExtensibleFuture<BackendConnection> tryAcquireConnection(@Nullable final String database) {
    if (acquiredConnections.incrementAndGet() > maxCapacity) {
      throw new TooManyBackendConnectionException(maxCapacity);
    }

    var connection = connectionQueue.poll();
    if (connection != null) {
      connection.assign();
      return new DoneFuture<>(connection);
    }

    ExtensibleFuture<BackendConnection> future;
    try {
      future = connectionFactory.newConnection(this);
      future.addListener(
          () -> {
            if (future.isSuccess()) {
              if (log.isDebugEnabled()) {
                log.info(
                    "A new backend connection has created. [database: {}, datasource: {}, connectionId: {}]",
                    database,
                    this.name,
                    future.getValue().connectionId());
              }
              future.getValue().assign();
            }
          },
          ThreadPool.DIRECT_EXECUTOR_SERVICE);
    } catch (BaseServerException e) {
      acquiredConnections.decrementAndGet();
      throw e;
    }
    return future;
  }

  @Override
  public void releaseConnection(final BackendConnection connection) {
    assert connection.isAssigned() : "Cannot release connection which is not assigned.";

    if (acquiredConnections.decrementAndGet() < 0) {
      throw new IllegalStateException("Repeatedly release the backend connection.");
    }
    if (!connection.isClosed()) {
      connectionQueue.offer(connection);
    }
  }

  @Override
  public void closeConnection(final BackendConnection connection) {
    if (connection.isAssigned()) {
      acquiredConnections.decrementAndGet();
    } else if (connection.isReleased()) {
      connectionQueue.remove(connection);
    }
  }

  @Override
  public ConnectionArguments getConnectionArguments() {
    return connectionArguments;
  }
}
