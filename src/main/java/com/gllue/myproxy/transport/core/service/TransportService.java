package com.gllue.myproxy.transport.core.service;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.command.result.query.QueryRowsConsumerPipeline;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.Promise;
import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.backend.datasource.DataSourceManager;
import com.gllue.myproxy.transport.protocol.packet.command.QueryCommandPacket;
import com.gllue.myproxy.transport.backend.command.BufferedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.DirectTransferQueryResultReader;
import com.gllue.myproxy.transport.backend.command.PipelineSupportedQueryResultReader;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransportService implements FrontendConnectionManager {
  @Getter
  private final DataSourceManager<BackendDataSource, BackendConnection> backendDataSourceManager;

  private final Map<Integer, FrontendConnection> frontendConnectionMap;

  public TransportService(final List<BackendDataSource> dataSources) {
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
        backendConnection.releaseOrClose();
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

  public void submitQueryAndDirectTransferResult(
      final int connectionId, final String query, final Callback<CommandResult> callback) {
    var frontendConnection = getFrontendConnection(connectionId);
    var backendConnection = frontendConnection.getBackendConnection();
    backendConnection.sendCommand(
        new QueryCommandPacket(query),
        new DirectTransferQueryResultReader(frontendConnection).addCallback(callback));
  }

  public void submitQueryToBackendDatabase(
      final int connectionId, final String query, final Callback<CommandResult> callback) {
    var connection = getFrontendConnection(connectionId).getBackendConnection();
    connection.sendCommand(
        new QueryCommandPacket(query), new BufferedQueryResultReader().addCallback(callback));
  }

  public Promise<CommandResult> submitQueryToBackendDatabase(
      final int connectionId, final String query) {
    return new Promise<>(
        (cb) -> {
          submitQueryToBackendDatabase(connectionId, query, cb);
        });
  }

  public void submitQueryToBackendDatabase(
      int connectionId, String query, QueryRowsConsumerPipeline pipeline) {
    var connection = getFrontendConnection(connectionId).getBackendConnection();
    connection.sendCommand(
        new QueryCommandPacket(query), new PipelineSupportedQueryResultReader(pipeline));
  }
}
