package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.core.connection.AbstractConnection;
import io.netty.channel.Channel;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendConnectionImpl extends AbstractConnection implements FrontendConnection {

  enum BindState {
    UNBOUND,
    BINDING,
    BOUND,
    CLOSED,
  }

  private final String dataSourceName;
  private final SessionContext sessionContext;
  private volatile BackendConnection backendConnection;
  private final AtomicReference<BindState> bindState = new AtomicReference<>(BindState.UNBOUND);

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
  public boolean bindBackendConnection(final BackendConnection backendConnection) {
    assert this.backendConnection == null && backendConnection != null;

    if (bindState.compareAndSet(BindState.UNBOUND, BindState.BINDING)) {
      this.backendConnection = backendConnection;
      return bindState.compareAndSet(BindState.BINDING, BindState.BOUND);
    }
    return false;
  }

  @Override
  public BackendConnection getBackendConnection() {
    return backendConnection;
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
  public void onCommandReceived() {
    updateLastAccessTime();
  }

  private void setBindStateToClose() {
    int waitCount = 0;
    do {
      if (bindState.compareAndSet(BindState.BOUND, BindState.CLOSED)) {
        break;
      }
      if (bindState.compareAndSet(BindState.UNBOUND, BindState.CLOSED)) {
        break;
      }

      if ((waitCount++ % 1024) == 0) {
        log.info("Waiting for the 'bindBackendConnection' function call to complete.");
        Thread.yield();
      }
    } while (true);
  }

  @Override
  protected void onClosed() {
    super.onClosed();
    try {
      sessionContext.close();
    } catch (Exception e) {
      log.error("An exception has occurred when closing session context.", e);
    }
    setBindStateToClose();
  }
}
