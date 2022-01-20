package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.DoneFuture;
import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.WrappedFuture;
import com.gllue.myproxy.common.exception.BaseServerException;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractConnectionPool implements ConnectionPool {
  public static final long DEFAULT_IDLE_TIMEOUT_IN_MILLS = TimeUnit.MINUTES.toMillis(30);
  public static final long DEFAULT_KEEP_ALIVE_TIME_IN_MILLS = TimeUnit.MINUTES.toMillis(3);
  public static final long DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS = TimeUnit.SECONDS.toMillis(3);
  public static final long DEFAULT_MAX_LIFE_TIME_IN_MILLS = TimeUnit.HOURS.toMillis(3);

  private static final long MIN_IDLE_TIMEOUT_IN_MILLS = TimeUnit.SECONDS.toMillis(30);
  private static final long MIN_KEEP_ALIVE_TIME_IN_MILLS = TimeUnit.SECONDS.toMillis(30);
  private static final long MIN_LIFE_TIME_IN_MILLS = TimeUnit.SECONDS.toMillis(30);
  private static final long MAX_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS = TimeUnit.SECONDS.toMillis(60);
  private static final double ALWAYS_ALIVE_RATIO = 0.1;

  private final AtomicBoolean isClosed = new AtomicBoolean();

  /**
   * The maximum size that the pool is allowed to reach, including both idle and in-use connections.
   */
  protected final int maxPoolSize;
  /** The maximum amount of time that a connection is allowed to sit idle in the pool. */
  protected final long idleTimeoutInMills;
  /**
   * How frequently the pool will attempt to keep a connection alive. To keep the connection alive,
   * a connectionTestQuery will be sent to the database when the keep alive time is reached. 0 meas
   * do not keep the connection alive.
   */
  protected final long keepAliveTimeInMills;
  /**
   * The maximum of time of the test query execution. If the test query execution timeout, the
   * connection will be closed.
   */
  protected final long keepAliveQueryTimeoutInMills;
  /**
   * The maximum lifetime of a connection in the pool. An in-use connection will never be retired,
   * only when it is closed will it then be removed.
   */
  protected final long maxLifeTimeInMills;

  private final AtomicInteger acquiredConnections = new AtomicInteger(0);

  private final ScheduledThreadPoolExecutor scheduler;
  private final ExecutorService executor;

  protected AbstractConnectionPool(
      final int maxPoolSize,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    this(
        maxPoolSize,
        DEFAULT_IDLE_TIMEOUT_IN_MILLS,
        DEFAULT_KEEP_ALIVE_TIME_IN_MILLS,
        DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
        DEFAULT_MAX_LIFE_TIME_IN_MILLS,
        scheduler,
        executor);
  }

  protected AbstractConnectionPool(
      final int maxPoolSize,
      final long idleTimeoutInMills,
      final long keepAliveTimeInMills,
      final long keepAliveQueryTimeoutInMills,
      final long maxLifeTimeInMills,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    Preconditions.checkArgument(maxPoolSize > 0, "maxPoolSize must > 0, got [%s]", maxPoolSize);
    Preconditions.checkArgument(
        keepAliveTimeInMills >= MIN_KEEP_ALIVE_TIME_IN_MILLS || keepAliveTimeInMills == 0,
        "keepAliveTimeInMills must equals to 0 or >= %s, got [%s]",
        MIN_KEEP_ALIVE_TIME_IN_MILLS,
        keepAliveTimeInMills);
    Preconditions.checkArgument(
        keepAliveQueryTimeoutInMills > 0
            && keepAliveQueryTimeoutInMills <= MAX_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
        "keepAliveQueryTimeoutInMills must > 0 and <= %s, got [%s]",
        MAX_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
        keepAliveQueryTimeoutInMills);
    Preconditions.checkArgument(
        maxLifeTimeInMills >= MIN_LIFE_TIME_IN_MILLS,
        "maxLifeTimeInMills must >= %s, got [%s]",
        MIN_LIFE_TIME_IN_MILLS,
        maxLifeTimeInMills);
    Preconditions.checkArgument(
        idleTimeoutInMills >= MIN_IDLE_TIMEOUT_IN_MILLS && idleTimeoutInMills <= maxLifeTimeInMills,
        "idleTimeoutInMills must >= %s and <= %s, got [%s]",
        MIN_IDLE_TIMEOUT_IN_MILLS,
        maxLifeTimeInMills,
        idleTimeoutInMills);

    this.maxPoolSize = maxPoolSize;
    this.idleTimeoutInMills = idleTimeoutInMills;
    this.keepAliveTimeInMills = keepAliveTimeInMills;
    this.keepAliveQueryTimeoutInMills = keepAliveQueryTimeoutInMills;
    this.maxLifeTimeInMills = maxLifeTimeInMills;
    this.scheduler = scheduler;
    this.executor = executor;

    scheduleIdleDetectionTask();
  }

  @Override
  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  @Override
  public int getNumberOfAcquiredConnections() {
    return acquiredConnections.get();
  }

  @Override
  public Connection acquireConnection(@Nullable String database) {
    var future = tryAcquireConnection(database);
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectionException(e);
    }
  }

  @Override
  public Connection acquireConnection(@Nullable String database, long timeout, TimeUnit unit) {
    var future = tryAcquireConnection(database);
    try {
      return future.get(timeout, unit);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new ConnectionException(e);
    }
  }

  /**
   * Create a new pooled connection.
   *
   * @param entry an entry object cached in pool.
   * @return a pooled connection.
   */
  abstract PooledConnection newPooledConnection(PoolEntry entry);

  /**
   * Create a new connection with the given database name.
   *
   * @param database database name
   * @return a future wrapped connection.
   */
  abstract ExtensibleFuture<Connection> newConnection(@Nullable String database);

  /**
   * Create a pool entry object with the given connection.
   *
   * @param connection connection.
   * @return pool entry that wraps the connection.
   */
  abstract PoolEntry newEntry(Connection connection);

  /**
   * Offer an unused pool entry to the connection pool.
   *
   * @param entry pool entry
   * @return true if the entry was added successfully.
   */
  abstract boolean offerEntry(PoolEntry entry);

  /**
   * Poll an unused pool entry from the connection pool. If the given database name is not null, it
   * should get a connection entry with the same database name as possible.
   *
   * @param database database name
   * @return unused pool entry that wraps the connection.
   */
  abstract PoolEntry pollEntry(@Nullable String database);

  /**
   * Reserve an entry if the entry is not in use.
   *
   * @param entry Pool entry
   * @return true if the entry is reserved.
   */
  abstract boolean reserveEntry(PoolEntry entry);

  /**
   * Unreserve an entry if the entry is reserved.
   *
   * @param entry Pool entry
   * @return true if the entry is unreserved.
   */
  abstract boolean unreserveEntry(PoolEntry entry);

  /**
   * Remove an entry from the pool if the entry is not in use.
   *
   * @param entry Pool entry
   * @return true if the entry is removed.
   */
  abstract boolean removeEntry(PoolEntry entry);

  /**
   * List all entries in the pool.
   *
   * @return all entries.
   */
  abstract Iterable<PoolEntry> entries();

  /**
   * A query statement that used to keep the connection alive.
   *
   * @return SQL string.
   */
  protected String connectionTestQuery() {
    return "SELECT 1";
  }

  private PoolEntry pollActiveEntry(String database) {
    PoolEntry entry;
    do {
      entry = pollEntry(database);
      if (entry == null) {
        return null;
      }

      var connection = entry.getConnection();
      if (connection.isClosed()) {
        entry = null;
      } else if (isConnectionNotAlive(connection)) {
        log.info(
            "Close connection [{}] in the connection pool because the max life time of connection has been reached.",
            connection.connectionId());
        entry = null;
        connection.close();
      }
    } while (entry == null);
    return entry;
  }

  @Override
  public ExtensibleFuture<Connection> tryAcquireConnection(@Nullable String database) {
    if (isClosed.get()) {
      throw new IllegalStateException("Connection pool has been closed.");
    }

    if (acquiredConnections.getAndIncrement() >= maxPoolSize) {
      acquiredConnections.decrementAndGet();
      throw new TooManyConnectionsException(maxPoolSize);
    }

    var connection = pollActiveEntry(database);
    if (connection != null) {
      return new DoneFuture<>(newPooledConnection(connection));
    }

    ExtensibleFuture<Connection> future;
    try {
      future = newConnection(database);
    } catch (BaseServerException e) {
      acquiredConnections.decrementAndGet();
      throw e;
    }
    return new PooledConnectionFuture(future);
  }

  private boolean isConnectionNotAlive(Connection connection) {
    return System.currentTimeMillis() - connection.createTime() > maxLifeTimeInMills;
  }

  @Override
  public void releaseConnection(Connection connection) {
    assert connection instanceof PooledConnection;

    if (isClosed.get()) {
      connection.close();
      acquiredConnections.decrementAndGet();
      return;
    }

    if (!connection.isClosed()) {
      var entry = ((PooledConnection) connection).getPoolEntry();
      var delegateConn = entry.getConnection();
      if (isConnectionNotAlive(delegateConn)) {
        log.info(
            "Close connection [{}] in the connection pool because the max life time of connection has been reached.",
            delegateConn.connectionId());
        delegateConn.close();
      } else {
        if (!offerEntry(entry)) {
          log.info(
              "Failed to offer a connection [{}] to the connection pool.",
              delegateConn.connectionId());
          delegateConn.close();
        }
      }
    }
    acquiredConnections.decrementAndGet();
  }

  @Override
  public void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      var entry = pollEntry(null);
      while (entry != null) {
        if (!entry.getConnection().isClosed()) {
          entry.getConnection().close();
        }
        entry = pollEntry(null);
      }
    }
  }

  private void scheduleKeepAliveTask(PoolEntry entry) {
    if (keepAliveTimeInMills <= 0) {
      return;
    }

    scheduler.schedule(
        () -> executor.execute(new ConnectionKeepAliveRunner(entry)),
        keepAliveTimeInMills,
        TimeUnit.MILLISECONDS);
  }

  private void scheduleIdleDetectionTask() {
    scheduler.scheduleWithFixedDelay(
        () -> executor.execute(new ConnectionIdleDetectionRunner()),
        idleTimeoutInMills,
        idleTimeoutInMills,
        TimeUnit.MILLISECONDS);
  }

  private void scheduleMaxLifeTimeDetectionTask(PoolEntry entry) {
    scheduler.schedule(
        () -> executor.execute(new ConnectionMaxLifeTimeDetectionRunner(entry)),
        maxLifeTimeInMills,
        TimeUnit.MILLISECONDS);
  }

  class PooledConnectionFuture extends WrappedFuture<Connection> {
    private volatile PooledConnection pooledConnection;

    PooledConnectionFuture(ExtensibleFuture<Connection> future) {
      super(future);
    }

    /**
     * Wraps a connection to a pooled connection object.
     *
     * @param connection the connection should be wrapped.
     * @return pooled connection.
     */
    private Connection wrappedConnection(Connection connection) {
      if (pooledConnection != null) {
        return pooledConnection;
      }

      synchronized (this) {
        if (pooledConnection == null) {
          var entry = newEntry(connection);
          scheduleKeepAliveTask(entry);
          scheduleMaxLifeTimeDetectionTask(entry);
          pooledConnection = newPooledConnection(entry);
        }
      }
      return pooledConnection;
    }

    @Override
    public Connection get() throws InterruptedException, ExecutionException {
      return wrappedConnection(super.get());
    }

    @Override
    public Connection get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return wrappedConnection(super.get(timeout, unit));
    }

    @Override
    public Connection getValue() {
      return wrappedConnection(super.getValue());
    }
  }

  @RequiredArgsConstructor
  class ConnectionKeepAliveRunner implements Runnable {
    private final PoolEntry poolEntry;

    private QueryCommandPacket buildTestQueryPacket() {
      return new QueryCommandPacket(connectionTestQuery());
    }

    private boolean isConnectionAlwaysAlive(Connection connection) {
      var maxDiff = keepAliveTimeInMills * ALWAYS_ALIVE_RATIO;
      return System.currentTimeMillis() - connection.lastAccessTime() <= maxDiff;
    }

    @Override
    public void run() {
      var connection = poolEntry.getConnection();
      if (isConnectionAlwaysAlive(connection)) {
        if (log.isDebugEnabled()) {
          log.debug(
              "The connection [{}] is always alive, no test query is sent.",
              connection.connectionId());
        }
        return;
      }

      if (reserveEntry(poolEntry)) {
        try {
          if (!connection.isClosed() && connection instanceof BackendConnection) {
            var backendConn = (BackendConnection) connection;
            if (log.isDebugEnabled()) {
              log.debug(
                  "Send a test query to the backend database to keep the connection [{}] alive.",
                  connection.connectionId());
            }
            var future = backendConn.sendCommand(buildTestQueryPacket());
            future.get(keepAliveQueryTimeoutInMills, TimeUnit.MILLISECONDS);
          }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
          connection.close();
          log.error("Failed to send test query to the backend database in the keep alive task.", e);
        } finally {
          if (connection.isClosed()) {
            removeEntry(poolEntry);
          } else {
            var res = unreserveEntry(poolEntry);
            assert res : "Failed to unreserve entry.";
            scheduleKeepAliveTask(poolEntry);
          }
        }
      }
    }
  }

  @RequiredArgsConstructor
  class ConnectionIdleDetectionRunner implements Runnable {
    private boolean isConnectionIdle(Connection connection, long currentTime) {
      return currentTime - connection.lastAccessTime() >= idleTimeoutInMills;
    }

    @Override
    public void run() {
      var currentTime = System.currentTimeMillis();
      var entries = entries();
      for (var entry : entries) {
        var connection = entry.getConnection();
        if (isConnectionIdle(connection, currentTime)) {
          if (removeEntry(entry)) {
            log.info(
                "Close the idle connection [{}] in the connection pool.",
                entry.getConnection().connectionId());
            connection.close();
          }
        } else if (connection.isClosed()) {
          log.info(
              "Remove closed connection [{}] in the connection pool.",
              entry.getConnection().connectionId());
          removeEntry(entry);
        }
      }
    }
  }

  @RequiredArgsConstructor
  class ConnectionMaxLifeTimeDetectionRunner implements Runnable {
    private final PoolEntry poolEntry;

    @Override
    public void run() {
      if (removeEntry(poolEntry)) {
        log.info(
            "Close connection [{}] in the connection pool because the max life time of connection has been reached.",
            poolEntry.getConnection().connectionId());
        poolEntry.getConnection().close();
      }
    }
  }
}
