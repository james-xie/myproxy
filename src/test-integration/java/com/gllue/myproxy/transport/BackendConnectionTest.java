package com.gllue.myproxy.transport;

import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.BaseIntegrationTest;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionListener;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.backend.BackendServer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 * <pre>
 * Prepare environment for mysql (8.0.25):
 *  CREATE DATABASE testdb;
 *  CREATE USER 'sha2'@'localhost' IDENTIFIED BY '1234';
 *  GRANT ALL PRIVILEGES ON * . * TO 'sha2'@'localhost';
 *  CREATE USER 'native'@'localhost' IDENTIFIED BY '1234';
 *  GRANT ALL PRIVILEGES ON * . * TO 'native'@'localhost';
 *  FLUSH PRIVILEGES;
 * </pre>
 */
public class BackendConnectionTest extends BaseIntegrationTest {
  static final String DATABASE = "testdb";

  public DataSource<BackendConnection> getDataSource(final String name) {
    var serverContext = getServerContext();
    var transportService = serverContext.getTransportService();
    var dataSource = transportService.getBackendDataSourceManager().getDataSource(name);
    Assert.assertNotNull(dataSource);
    return dataSource;
  }

  @Test
  public void testConnectUsingNativePasswordPlugin()
      throws InterruptedException, TimeoutException, ExecutionException {
    var backendServer = BackendServer.getInstance();

    var future = new PlainFuture<Boolean>();
    var listener =
        new BackendConnectionListener() {

          @Override
          public boolean onConnected(@Nonnull BackendConnection connection) {
            future.set(true);
            return true;
          }

          @Override
          public void onConnectFailed(Exception e) {
            future.set(false);
          }

          @Override
          public void onClosed(@Nullable BackendConnection connection) {
            future.set(false);
          }
        };

    // "native" user using "native-password" auth plugin.
    var connArgsForNativeUser = getDataSource("s2").getConnectionArguments(DATABASE);
    backendServer.connect(connArgsForNativeUser, listener);
    assertTrue(future.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testConnectUsingCachingSha2Plugin()
      throws InterruptedException, TimeoutException, ExecutionException {
    var backendServer = BackendServer.getInstance();

    var future = new PlainFuture<Boolean>();
    var listener =
        new BackendConnectionListener() {

          @Override
          public boolean onConnected(@Nonnull BackendConnection connection) {
            future.set(true);
            return true;
          }

          @Override
          public void onConnectFailed(Exception e) {
            future.set(false);
          }

          @Override
          public void onClosed(@Nullable BackendConnection connection) {
            future.set(false);
          }
        };

    // "root" user using "caching-sha2" auth plugin.
    var connArgsForRootUser = getDataSource("s1").getConnectionArguments(DATABASE);
    backendServer.connect(connArgsForRootUser, listener);
    assertTrue(future.get(5, TimeUnit.SECONDS));
  }
}
