package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.common.concurrent.ExtensibleFuture;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.backend.BackendServer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BackendConnectionFactory {

  private BackendServer backendServer() {
    return BackendServer.getInstance();
  }

  public ExtensibleFuture<BackendConnection> newConnection(
      final DataSource<BackendConnection> dataSource) {
    PlainFuture<BackendConnection> future = new PlainFuture<>();

    BackendConnectionListener backendConnectionListener =
        new BackendConnectionListener() {
          @Override
          public boolean onConnected(@Nonnull BackendConnection connection) {
            if (future.isDone()) {
              return false;
            }

            connection.setDataSource(dataSource);
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
                    connection.dataSource().getName());
              } else {
                log.error("Connection abnormally close.");
              }
              future.setException(new BackendConnectionClosedException());
            }

            if (connection != null) {
              connection.close();
            }
          }
        };

    backendServer()
        .connect(dataSource.getConnectionArguments(), backendConnectionListener)
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
