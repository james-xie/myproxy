package com.gllue.myproxy.transport.frontend.connection;

import com.gllue.myproxy.transport.backend.connection.BackendConnection;
import com.gllue.myproxy.transport.core.connection.AbstractConnection;
import io.netty.channel.Channel;
import java.lang.ref.WeakReference;

public class FrontendConnectionImpl extends AbstractConnection implements FrontendConnection {

  private final String dataSourceName;
  private volatile WeakReference<BackendConnection> backendConnectionRef;

  public FrontendConnectionImpl(
      final int connectionId, final Channel channel, final String dataSource) {
    super(connectionId, channel);
    this.dataSourceName = dataSource;
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
}
