package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.transport.backend.BackendServer;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.core.service.TransportService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BackendConnectionFactory {
  private final TransportService transportService;

  private BackendServer backendServer() {
    return BackendServer.getInstance();
  }

  public ExtensibleFuture<Connection> newConnection(
      final DataSource dataSource, final String database) {
    PlainFuture<Connection> future = new PlainFuture<>();

    BackendConnectionListener backendConnectionListener =
        new BackendConnectionListener() {
          @Override
          public boolean onConnected(@Nonnull BackendConnection connection) {
            if (future.isDone()) {
              return false;
            }

            connection.setDataSourceName(dataSource.getName());
            return future.set(connection);
          }

          @Override
          public void onConnectFailed(Exception e) {
            future.setException(e);
          }

          @Override
          public void onClosed(@Nullable BackendConnection connection) {
            if (!future.isDone()) {
              if (connection != null) {
                log.error(
                    "Connection abnormally close. [connectionId={}, datasource={}]",
                    connection.connectionId(),
                    connection.getDataSourceName());
              } else {
                log.error("Connection abnormally close.");
              }
              future.setException(new BackendConnectionClosedException());
            }

            if (connection != null) {
              connection.close();
              transportService.removeBackendConnection(connection.connectionId());
            }
          }
        };

    backendServer()
        .connect(dataSource.getConnectionArguments(database), backendConnectionListener)
        .addListener(
            (f) -> {
              if (f.isCancelled()) {
                future.cancel(false);
              } else if (!f.isSuccess()) {
                future.setException(f.cause());
              }
            });

    return future;
  }
}
