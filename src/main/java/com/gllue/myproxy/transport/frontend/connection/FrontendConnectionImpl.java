package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.core.connection.AbstractConnection;
import io.netty.channel.Channel;
import java.lang.ref.WeakReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendConnectionImpl extends AbstractConnection implements FrontendConnection {

  private final String dataSourceName;
  private final SessionContext sessionContext;
  private volatile WeakReference<BackendConnection> backendConnectionRef;

  public FrontendConnectionImpl(
      final int connectionId, final String user, final Channel channel, final String dataSource) {
    super(connectionId, user, channel);
    this.dataSourceName = dataSource;
    this.sessionContext = new SessionContextImpl(this);
  }

  @Override
  public String getDataSourceName() {
    return dataSourceName;
  }

  @Override
  public void bindBackendConnection(final BackendConnection backendConnection) {
    backendConnectionRef = new WeakReference<>(backendConnection);
  }

  @Override
  public BackendConnection getBackendConnection() {
    if (backendConnectionRef == null) {
      return null;
    }
    return backendConnectionRef.get();
  }

  @Override
  public void changeDatabase(String database) {
    var currentDb = currentDatabase();
    if (currentDb == null || !currentDb.equals(database)) {
      super.changeDatabase(database);
      // Clean the encryption key when the database is changed.
      sessionContext.setEncryptKey(null);
    }
  }

  @Override
  public SessionContext getSessionContext() {
    return sessionContext;
  }

  @Override
  protected void onClosed() {
    super.onClosed();
    try {
      sessionContext.close();
    } catch (Exception e) {
      log.error("An exception has occurred when closing session context.", e);
    }
  }
}
