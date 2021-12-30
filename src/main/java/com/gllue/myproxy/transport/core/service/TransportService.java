package com.gllue.myproxy.transport.core.service;

import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.BEGIN_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.COMMIT_COMMAND;
import static com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket.ROLLBACK_COMMAND;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.QueryRowsConsumerPipeline;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.transport.backend.command.CachedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.backend.command.DefaultCommandResultReader;
import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.backend.datasource.DataSourceManager;
import com.gllue.myproxy.transport.exception.ExceptionResolver;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import com.gllue.myproxy.transport.backend.command.BufferedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.DirectTransferQueryResultReader;
import com.gllue.myproxy.transport.backend.command.PipelineSupportedQueryResultReader;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionManager;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransportService implements FrontendConnectionManager {
  private final Configurations configurations;

  @Getter
  private final DataSourceManager<BackendDataSource, BackendConnection> backendDataSourceManager;

  private final Map<Integer, FrontendConnection> frontendConnectionMap;

  public TransportService(
      final Configurations configurations, final List<BackendDataSource> dataSources) {
    this.configurations = configurations;
    backendDataSourceManager = new DataSourceManager<>(dataSources);
    frontendConnectionMap = new ConcurrentHashMap<>();
  }

  public void registerFrontendConnection(final FrontendConnection connection) {
    var old = frontendConnectionMap.putIfAbsent(connection.connectionId(), connection);
    if (old != null) {
      throw new IllegalArgumentException(
          String.format("Frontend connection already exists. [%s]", connection.connectionId()));
    }
  }

  public FrontendConnection removeFrontendConnection(final int connectionId) {
    var connection = frontendConnectionMap.remove(connectionId);
    if (connection != null) {
      var backendConnection = connection.getBackendConnection();
      if (backendConnection != null) {
        // if the transaction is not committed before the connection is released, the transaction
        // should be forced to rollback.
        if (!backendConnection.isClosed() && backendConnection.isTransactionOpened()) {
          backendConnection
              .sendCommand(ROLLBACK_COMMAND)
              .addListener(backendConnection::releaseOrClose, ThreadPool.DIRECT_EXECUTOR_SERVICE);
        } else {
          backendConnection.releaseOrClose();
        }
      }
    }
    return connection;
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

  public ExtensibleFuture<BackendConnection> assignBackendConnection(
      final FrontendConnection frontendConnection) {
    var future =
        backendDataSourceManager.getBackendConnection(
            frontendConnection.getDataSourceName(), frontendConnection.currentDatabase());
    if (future == null) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot get backend connection, maybe the data source [%s] is not found.",
              frontendConnection.getDataSourceName()));
    }

    future.addListener(
        () -> {
          if (future.isSuccess()) {
            if (frontendConnection.isClosed()) {
              future.getValue().releaseOrClose();
            } else {
              frontendConnection.bindBackendConnection(future.getValue());
            }
          }
        },
        ThreadPool.DIRECT_EXECUTOR_SERVICE);
    return future;
  }

  public Promise<Boolean> beginTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var reader = new DefaultCommandResultReader().addCallback(cb);
              backendConnection.sendCommand(BEGIN_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.begin();
              frontendConnection.begin();
              return true;
            });
  }

  public Promise<Boolean> commitTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var reader = new DefaultCommandResultReader().addCallback(cb);
              backendConnection.sendCommand(COMMIT_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.commit();
              frontendConnection.commit();
              return true;
            });
  }

  public Promise<Boolean> rollbackTransaction(final int connectionId) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    return new Promise<CommandResult>(
            (cb) -> {
              var reader = new DefaultCommandResultReader().addCallback(cb);
              backendConnection.sendCommand(ROLLBACK_COMMAND, reader);
            })
        .then(
            (result) -> {
              backendConnection.rollback();
              frontendConnection.rollback();
              return true;
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

  public Promise<CommandResult> submitQueryToBackendDatabase(
      final int connectionId, final String query) {
    return new Promise<>(
        (cb) -> {
          submitQueryToBackendDatabase(connectionId, query, cb);
        });
  }

  public Promise<CommandResult> kill(
      final int connectionId, final int threadId, final boolean killQuery) {
    return new Promise<>(
        (cb) -> {
          int backendThreadId =
              getFrontendConnection(threadId).getBackendConnection().connectionId();
          String query;
          if (killQuery) {
            query = "KILL QUERY " + backendThreadId;
          } else {
            query = "KILL CONNECTION " + backendThreadId;
          }
          submitQueryToBackendDatabase(connectionId, query, cb);
        });
  }
}
