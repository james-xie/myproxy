package com.gllue.myproxy.transport.core.service;

import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.BEGIN_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.COMMIT_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.DISABLE_AUTO_COMMIT_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.ENABLE_AUTO_COMMIT_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.ROLLBACK_COMMAND;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.concurrent.ThreadPool.Name;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.config.TransportConfigPropertyKey;
import com.gllue.myproxy.transport.backend.command.CachedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.backend.command.DefaultCommandResultReader;
import com.gllue.myproxy.transport.backend.command.DirectTransferQueryResultReader;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.backend.connection.FIFOBackendConnectionPool;
import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.backend.datasource.DataSourceManager;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.core.connection.ConnectionPool;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import com.google.common.base.Preconditions;
import io.prometheus.client.Gauge;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransportService implements AutoCloseable {
  private static final Gauge frontendConnections =
      Gauge.build().name("frontend_connections").help("Frontend connections.").register();
  private static final Gauge backendConnections =
      Gauge.build().name("backend_connections").help("Backend connections.").register();

  private final Configurations configurations;
  private final ThreadPool threadPool;

  @Getter private DataSourceManager<BackendDataSource> backendDataSourceManager;

  private final Map<Integer, FrontendConnection> frontendConnectionMap;
  private final Map<Integer, BackendConnection> backendConnectionMap;
  private Map<String, ConnectionPool> backendConnectionPool;

  public TransportService(final Configurations configurations, final ThreadPool threadPool) {
    this.configurations = configurations;
    this.threadPool = threadPool;
    this.frontendConnectionMap = new ConcurrentHashMap<>();
    this.backendConnectionMap = new ConcurrentHashMap<>();
  }

  private long maxFrontendConnectionIdleTimeInMills() {
    return configurations.getValue(
        Type.TRANSPORT, TransportConfigPropertyKey.FRONTEND_CONNECTION_MAX_IDLE_TIME_IN_MILLS);
  }

  private int idleConnectionDetectInterval() {
    return configurations.getValue(
        Type.TRANSPORT,
        TransportConfigPropertyKey.FRONTEND_CONNECTION_IDLE_DETECT_INTERVAL_SECONDS);
  }

  private int maxBackendConnectionPoolSize() {
    return configurations.getValue(
        Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_CONNECTION_POOL_SIZE);
  }

  private long backendConnectionIdleTimeoutInMills() {
    return configurations.getValue(
        Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_CONNECTION_IDLE_TIMEOUT_IN_MILLS);
  }

  private long backendConnectionKeepAliveTimeInMills() {
    return configurations.getValue(
        Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_CONNECTION_KEEP_ALIVE_TIME_IN_MILLS);
  }

  private long backendConnectionKeepAliveQueryTimeoutInMills() {
    return configurations.getValue(
        Type.TRANSPORT,
        TransportConfigPropertyKey.BACKEND_CONNECTION_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS);
  }

  private long backendConnectionMaxLifeTimeInMills() {
    return configurations.getValue(
        Type.TRANSPORT, TransportConfigPropertyKey.BACKEND_CONNECTION_MAX_LIFE_TIME_IN_MILLS);
  }

  public void initialize(final List<BackendDataSource> dataSources) {
    if (this.backendDataSourceManager != null) {
      throw new IllegalStateException("Cannot override backendDataSourceManager");
    }
    this.backendDataSourceManager = new DataSourceManager<>(dataSources);
    this.backendConnectionPool = buildBackendConnectionPool(this.backendDataSourceManager);
    this.scheduleIdleFrontendConnectionDetection();
  }

  private Map<String, ConnectionPool> buildBackendConnectionPool(
      DataSourceManager<BackendDataSource> dataSourceManager) {
    Map<String, ConnectionPool> poolMap = new HashMap<>();
    for (var name : dataSourceManager.getDataSourceNames()) {
      var dataSource = dataSourceManager.getDataSource(name);
      assert dataSource != null;

      var pool =
          new FIFOBackendConnectionPool(
              dataSource.getConnectionArguments(null),
              (arguments, database) -> dataSource.tryGetConnection(database),
              maxBackendConnectionPoolSize(),
              backendConnectionIdleTimeoutInMills(),
              backendConnectionKeepAliveTimeInMills(),
              backendConnectionKeepAliveQueryTimeoutInMills(),
              backendConnectionMaxLifeTimeInMills(),
              threadPool.getScheduler(),
              threadPool.executor(Name.GENERIC));
      poolMap.put(name, pool);
    }
    return Collections.unmodifiableMap(poolMap);
  }

  private void scheduleIdleFrontendConnectionDetection() {
    var executor = threadPool.executor(Name.GENERIC);
    var maxIdleTime = maxFrontendConnectionIdleTimeInMills();
    threadPool.scheduleWithFixedDelay(
        new IdleConnectionDetector(maxIdleTime, frontendConnectionMap.values()),
        maxIdleTime,
        idleConnectionDetectInterval(),
        TimeUnit.MILLISECONDS,
        executor);
  }

  public void registerFrontendConnection(final FrontendConnection connection) {
    var old = frontendConnectionMap.putIfAbsent(connection.connectionId(), connection);
    if (old != null) {
      throw new IllegalArgumentException(
          String.format("Frontend connection already exists. [%s]", connection.connectionId()));
    }
    frontendConnections.inc();
  }

  public void removeFrontendConnection(final int connectionId) {
    var connection = frontendConnectionMap.remove(connectionId);
    if (connection != null) {
      frontendConnections.dec();
      var backendConnection = connection.getBackendConnection();
      if (backendConnection != null) {
        // if the transaction is not committed before the connection is released, the transaction
        // should be forced to rollback.
        if (!backendConnection.isClosed() && backendConnection.isTransactionOpened()) {
          backendConnection
              .sendCommand(ROLLBACK_COMMAND)
              .addListener(backendConnection::close, ThreadPool.DIRECT_EXECUTOR_SERVICE);
        } else {
          backendConnection.close();
        }
      }
    }
  }

  private void registerBackendConnection(final BackendConnection connection) {
    var old = backendConnectionMap.putIfAbsent(connection.connectionId(), connection);
    if (old != null) {
      throw new IllegalArgumentException(
          String.format("Backend connection already exists. [%s]", connection.connectionId()));
    }
    backendConnections.inc();
  }

  public void removeBackendConnection(final int connectionId) {
    var backendConnection = backendConnectionMap.remove(connectionId);
    if (backendConnection != null) {
      backendConnections.dec();
      var frontendConnection = backendConnection.getFrontendConnection();
      if (frontendConnection != null) {
        frontendConnection.close();
      }
    }
  }

  public FrontendConnection getFrontendConnection(final int connectionId) {
    var connection = frontendConnectionMap.get(connectionId);
    if (connection == null) {
      throw new IllegalArgumentException(
          String.format("Invalid connectionId argument. [%s]", connectionId));
    }
    return connection;
  }

  public void closeFrontendConnection(final int connectionId) {
    getFrontendConnection(connectionId).close();
  }

  public ExtensibleFuture<Connection> assignBackendConnection(
      final FrontendConnection frontendConnection) {
    var future =
        backendConnectionPool
            .get(frontendConnection.getDataSourceName())
            .tryAcquireConnection(frontendConnection.currentDatabase());
    if (future == null) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot get backend connection, maybe the data source [%s] is not found.",
              frontendConnection.getDataSourceName()));
    }

    future.addListener(
        () -> {
          if (future.isSuccess()) {
            var backendConnection = (BackendConnection) future.getValue();
            registerBackendConnection(backendConnection);
            if (frontendConnection.isClosed()) {
              backendConnection.close();
            } else {
              frontendConnection.bindBackendConnection(backendConnection);
              backendConnection.bindFrontendConnection(frontendConnection);
            }
          }
        },
        ThreadPool.DIRECT_EXECUTOR_SERVICE);
    return future;
  }

  public Promise<CommandResult> beginTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var newCallback = wrappedCallback(frontendConnection, cb);
              var reader = new DefaultCommandResultReader().addCallback(newCallback);
              backendConnection.sendCommand(BEGIN_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.begin();
              frontendConnection.begin();
              return result;
            });
  }

  public Promise<CommandResult> commitTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var newCallback = wrappedCallback(frontendConnection, cb);
              var reader = new DefaultCommandResultReader().addCallback(newCallback);
              backendConnection.sendCommand(COMMIT_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.commit();
              frontendConnection.commit();
              return result;
            });
  }

  public Promise<CommandResult> rollbackTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var newCallback = wrappedCallback(frontendConnection, cb);
              var reader = new DefaultCommandResultReader().addCallback(newCallback);
              backendConnection.sendCommand(ROLLBACK_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.rollback();
              frontendConnection.rollback();
              return result;
            });
  }

  public Promise<CommandResult> setAutoCommit(final int connectionId, final boolean autoCommit) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var newCallback = wrappedCallback(frontendConnection, cb);
              var reader = new DefaultCommandResultReader().addCallback(newCallback);
              QueryCommandPacket command =
                  autoCommit ? ENABLE_AUTO_COMMIT_COMMAND : DISABLE_AUTO_COMMIT_COMMAND;
              backendConnection.sendCommand(command, reader);
            })
        .then(
            (result) -> {
              if (autoCommit) {
                backendConnection.enableAutoCommit();
                frontendConnection.enableAutoCommit();
              } else {
                backendConnection.disableAutoCommit();
                frontendConnection.disableAutoCommit();
              }
              return result;
            });
  }

  private Callback<CommandResult> wrappedCallback(
      FrontendConnection connection, Callback<CommandResult> callback) {
    return new Callback<>() {
      @Override
      public void onSuccess(CommandResult result) {
        try {
          callback.onSuccess(result);
        } catch (Exception e) {
          callback.onFailure(e);
        }
      }

      @Override
      public void onFailure(Throwable e) {
        try {
          callback.onFailure(e);
        } catch (Exception exception) {
          log.error(
              "An exception has occurred during invoking the callback.onFailure method.",
              exception);
          connection.close();
        }
      }

      public Executor executor() {
        var executor = callback.executor();
        if (executor == null) {
          executor = threadPool.executor(Name.COMMAND);
        }
        return executor;
      }
    };
  }

  public void submitQueryAndDirectTransferResult(
      final int connectionId, final String query, final Callback<CommandResult> callback) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    var newCallback = wrappedCallback(frontendConnection, callback);
    backendConnection.sendCommand(
        new QueryCommandPacket(query),
        new DirectTransferQueryResultReader(frontendConnection).addCallback(newCallback));
  }

  private CommandResultReader newCachedQueryResultReader(Callback<CommandResult> callback) {
    int maxCapacity =
        configurations.getValue(
            Type.GENERIC, GenericConfigPropertyKey.QUERY_RESULT_CACHED_MAX_CAPACITY_IN_BYTES);
    return new CachedQueryResultReader(maxCapacity).addCallback(callback);
  }

  public void submitQueryToBackendDatabase(
      final int connectionId, final String query, final Callback<CommandResult> callback) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    var newCallback = wrappedCallback(frontendConnection, callback);
    backendConnection.sendCommand(
        new QueryCommandPacket(query), newCachedQueryResultReader(newCallback));
  }

  public Promise<CommandResult> kill(
      final int connectionId, final int threadId, final boolean killQuery) {
    return new Promise<>(
        (cb) -> {
          var backendThreadId =
              getFrontendConnection(threadId).getBackendConnection().getDatabaseThreadId();
          String query;
          if (killQuery) {
            query = "KILL QUERY " + backendThreadId;
          } else {
            query = "KILL CONNECTION " + backendThreadId;
          }
          submitQueryToBackendDatabase(connectionId, query, cb);
        });
  }

  public Promise<CommandResult> useDatabase(final int connectionId, final String dbName) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var newCallback = wrappedCallback(frontendConnection, cb);
              var reader = new DefaultCommandResultReader().addCallback(newCallback);
              var query = String.format("USE `%s`", dbName);
              backendConnection.sendCommand(new QueryCommandPacket(query), reader);
            })
        .then(
            (result) -> {
              backendConnection.changeDatabase(dbName);
              frontendConnection.changeDatabase(dbName);
              return result;
            });
  }

  @Override
  public void close() throws Exception {
    for (var connection : frontendConnectionMap.values()) {
      if (!connection.isClosed()) {
        connection.close();
      }
    }

    for (var connection : backendConnectionMap.values()) {
      if (!connection.isClosed()) {
        connection.close();
      }
    }
  }

  @Getter
  @RequiredArgsConstructor
  public static class ConnectionInfo {
    private final int frontendConnectionId;
    private final int backendConnectionId;
    private final String user;
    private final String database;
    private final SocketAddress clientSocketAddress;
  }

  public List<ConnectionInfo> getConnectionInfoList(final String datasource) {
    Preconditions.checkNotNull(datasource);

    var result = new ArrayList<ConnectionInfo>();
    for (var frontendConn : frontendConnectionMap.values()) {
      if (!datasource.equals(frontendConn.getDataSourceName())) {
        continue;
      }

      var backendConn = frontendConn.getBackendConnection();
      result.add(
          new ConnectionInfo(
              frontendConn.connectionId(),
              backendConn.connectionId(),
              frontendConn.currentUser(),
              frontendConn.currentDatabase(),
              frontendConn.remoteAddress()));
    }
    return result;
  }

  @RequiredArgsConstructor
  static class IdleConnectionDetector implements Runnable {
    private final long maxIdleTimeInMills;
    private final Iterable<? extends Connection> connectionIterable;

    private boolean isConnectionIdle(Connection connection, long currentTime) {
      return currentTime - connection.lastAccessTime() >= maxIdleTimeInMills;
    }

    private void closeIdleConnections() {
      var currentTime = System.currentTimeMillis();
      for (var connection : connectionIterable) {
        if (!isConnectionIdle(connection, currentTime)) {
          continue;
        }

        connection.close();
        log.info("Close the idle connection [{}].", connection.connectionId());
      }
    }

    @Override
    public void run() {
      try {
        closeIdleConnections();
      } catch (Exception e) {
        log.error("Failed to close idle connections.", e);
      }
    }
  }
}
