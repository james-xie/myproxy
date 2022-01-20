package com.gllue.myproxy.transport.backend.connection;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.core.connection.ConnectionFactory;
import com.gllue.myproxy.transport.core.connection.ConnectionPool;
import com.gllue.myproxy.transport.core.connection.FIFOConnectionPool;
import com.gllue.myproxy.transport.core.connection.PooledConnection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Consumer;

public class FIFOBackendConnectionPool extends FIFOConnectionPool {

  public FIFOBackendConnectionPool(
      final ConnectionArguments arguments,
      ConnectionFactory connectionFactory,
      final int maxPoolSize,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    this(
        arguments,
        connectionFactory,
        maxPoolSize,
        DEFAULT_IDLE_TIMEOUT_IN_MILLS,
        DEFAULT_KEEP_ALIVE_TIME_IN_MILLS,
        DEFAULT_KEEP_ALIVE_QUERY_TIMEOUT_IN_MILLS,
        DEFAULT_MAX_LIFE_TIME_IN_MILLS,
        scheduler,
        executor);
  }

  public FIFOBackendConnectionPool(
      final ConnectionArguments arguments,
      final ConnectionFactory connectionFactory,
      final int maxPoolSize,
      final long idleTimeoutInMills,
      final long keepAliveTimeInMills,
      final long keepAliveQueryTimeoutInMills,
      final long maxLifeTimeInMills,
      final ScheduledThreadPoolExecutor scheduler,
      final ExecutorService executor) {
    super(
        arguments,
        connectionFactory,
        maxPoolSize,
        idleTimeoutInMills,
        keepAliveTimeInMills,
        keepAliveQueryTimeoutInMills,
        maxLifeTimeInMills,
        scheduler,
        executor);
  }

  @Override
  public PooledConnection newPooledConnection(PoolEntry entry) {
    return new PooledBackendConnection(entry, this);
  }

  static class PooledBackendConnection extends PooledConnection implements BackendConnection {
    private final BackendConnection backendConnection;

    public PooledBackendConnection(PoolEntry entry, ConnectionPool pool) {
      super(entry, pool);
      this.backendConnection = (BackendConnection) entry.getConnection();
    }

    @Override
    public void setDataSourceName(String dataSourceName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDataSourceName() {
      return backendConnection.getDataSourceName();
    }

    @Override
    public long getDatabaseThreadId() {
      return backendConnection.getDatabaseThreadId();
    }

    @Override
    public void bindFrontendConnection(FrontendConnection frontendConnection) {
      backendConnection.bindFrontendConnection(frontendConnection);
    }

    @Override
    public void unbindFrontendConnection() {
      backendConnection.unbindFrontendConnection();
    }

    @Override
    public FrontendConnection getFrontendConnection() {
      return backendConnection.getFrontendConnection();
    }

    @Override
    public void setCommandResultReader(CommandResultReader reader) {
      backendConnection.setCommandResultReader(reader);
    }

    @Override
    public CommandResultReader getCommandResultReader() {
      return backendConnection.getCommandResultReader();
    }

    @Override
    public boolean readingResponse() {
      return backendConnection.readingResponse();
    }

    @Override
    public void onResponseReceived() {
      backendConnection.onResponseReceived();
    }

    @Override
    public void setCommandExecutionDone() {
      backendConnection.setCommandExecutionDone();
    }

    @Override
    public void sendCommand(CommandPacket packet, CommandResultReader reader) {
      backendConnection.sendCommand(packet, reader);
    }

    @Override
    public ListenableFuture<CommandResult> sendCommand(CommandPacket packet) {
      return backendConnection.sendCommand(packet);
    }

    @Override
    public void reset(Callback<CommandResult> callback) {
      backendConnection.reset(callback);
    }

    @Override
    public void close(Consumer<Connection> onClosed) {
      if (!backendConnection.isClosed() && backendConnection.readingResponse()) {
        // If the command is being executed, we should close the backend connection
        // because the state is undefined. Otherwise the backend connection can be reused.
        backendConnection.close();
      }

      super.close(
          (conn) -> {
            unbindFrontendConnection();
            onClosed.accept(conn);
          });
    }
  }
}
