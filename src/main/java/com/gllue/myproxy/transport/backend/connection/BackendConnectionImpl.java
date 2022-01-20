package com.gllue.myproxy.transport.backend.connection;

import static com.gllue.myproxy.constant.TimeConstants.NANOS_PER_SECOND;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.common.concurrent.SettableFuture;
import com.gllue.myproxy.transport.backend.command.CachedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.backend.command.DefaultCommandResultReader;
import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.core.connection.AbstractConnection;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnection;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.SimpleCommandPacket;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.prometheus.client.Summary;
import java.lang.ref.WeakReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendConnectionImpl extends AbstractConnection implements BackendConnection {
  private static final Summary backendProcessingLatency =
      Summary.build()
          .name("backend_processing_latency_summary")
          .help("Backend processing latency summary in seconds.")
          .unit("second")
          .register();
  private static final Summary receivingResponseLatency =
      Summary.build()
          .name("receiving_response_latency_summary")
          .help("Receiving response latency summary in seconds.")
          .unit("second")
          .register();

  private final long databaseThreadId;
  private String dataSourceName;

  private volatile CommandResultReader commandResultReader;
  private volatile CommandResultReader newCommandResultReader;
  private volatile WeakReference<FrontendConnection> frontendConnectionRef;
  private volatile boolean firstResponse;
  private volatile long sendCommandTime;
  private long receiveFirstResponseTime;

  public BackendConnectionImpl(
      final int connectionId,
      final String user,
      final Channel channel,
      final long databaseThreadId) {
    super(connectionId, user, channel);
    this.databaseThreadId = databaseThreadId;
  }

  public BackendConnectionImpl(
      final int connectionId,
      final String user,
      final Channel channel,
      final long databaseThreadId,
      final String dataSourceName) {
    this(connectionId, user, channel, databaseThreadId);
    setDataSourceName(dataSourceName);
  }

  @Override
  public void setDataSourceName(final String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  @Override
  public long getDatabaseThreadId() {
    return databaseThreadId;
  }

  @Override
  public void bindFrontendConnection(FrontendConnection frontendConnection) {
    assert frontendConnectionRef == null;
    frontendConnectionRef = new WeakReference<>(frontendConnection);
  }

  @Override
  public void unbindFrontendConnection() {
    frontendConnectionRef = null;
  }

  @Override
  public FrontendConnection getFrontendConnection() {
    if (frontendConnectionRef == null) {
      return null;
    }
    return frontendConnectionRef.get();
  }

  @Override
  public String getDataSourceName() {
    Preconditions.checkNotNull(dataSourceName);
    return dataSourceName;
  }

  @Override
  public void setCommandExecutionDone() {
    if (log.isDebugEnabled()) {
      log.debug("SQL execution done.");
    }

    assert commandResultReader != null;
    var reader = commandResultReader;
    try {
      reader.close();
    } catch (Exception e1) {
      log.error("An error was occurred when closing the command result reader.", e1);
    } finally {
      commandResultReader = null;
    }

    // We must invoke fireReadCompletedEvent at the end of the method.
    reader.fireReadCompletedEvent();

    receivingResponseLatency.observe(
        (System.nanoTime() - receiveFirstResponseTime) / NANOS_PER_SECOND);
  }

  @Override
  public boolean readingResponse() {
    return commandResultReader != null;
  }

  @Override
  public void onResponseReceived() {
    if (firstResponse) {
      receiveFirstResponseTime = System.nanoTime();
      backendProcessingLatency.observe(
          (receiveFirstResponseTime - sendCommandTime) / NANOS_PER_SECOND);
      firstResponse = false;
    }
  }

  @Override
  public void write(final MySQLPacket packet) {
    assert packet instanceof CommandPacket;
    super.write(packet);
  }

  @Override
  public void writeAndFlush(final MySQLPacket packet) {
    assert packet instanceof CommandPacket;
    super.writeAndFlush(packet);
  }

  @Override
  public void write(final MySQLPacket packet, final SettableFuture<Connection> future) {
    assert packet instanceof CommandPacket;
    super.write(packet, future);
  }

  @Override
  public void writeAndFlush(final MySQLPacket packet, final SettableFuture<Connection> future) {
    assert packet instanceof CommandPacket;
    super.writeAndFlush(packet, future);
  }

  @Override
  public void setCommandResultReader(CommandResultReader reader) {
    if (newCommandResultReader != null) {
      throw new IllegalStateException("Cannot override commandResultReader.");
    }
    newCommandResultReader = reader;
    reader.bindConnection(this);
  }

  @Override
  public CommandResultReader getCommandResultReader() {
    if (commandResultReader != null && newCommandResultReader != null) {
      throw new IllegalStateException("Cannot override commandResultReader.");
    }
    if (newCommandResultReader != null) {
      commandResultReader = newCommandResultReader;
      newCommandResultReader = null;
    }
    return commandResultReader;
  }

  @Override
  public void sendCommand(CommandPacket packet, CommandResultReader reader) {
    firstResponse = true;
    sendCommandTime = System.nanoTime();
    setCommandResultReader(reader);
    writeAndFlush(packet);
    updateLastAccessTime();
  }

  @Override
  public ListenableFuture<CommandResult> sendCommand(CommandPacket packet) {
    var future = new PlainFuture<CommandResult>();
    sendCommand(
        packet,
        CachedQueryResultReader.newInstance(
            new Callback<>() {
              @Override
              public void onSuccess(CommandResult result) {
                future.set(result);
              }

              @Override
              public void onFailure(Throwable e) {
                future.setException(e);
              }
            }));
    return future;
  }

  @Override
  public void reset(Callback<CommandResult> callback) {
    sendCommand(
        new SimpleCommandPacket(MySQLCommandPacketType.COM_RESET_CONNECTION),
        DefaultCommandResultReader.newInstance(callback));
  }

  @Override
  protected void onClosed() {
    super.onClosed();
    unbindFrontendConnection();
  }
}
