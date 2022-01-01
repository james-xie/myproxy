package com.gllue.myproxy.transport.backend.connection;

import static com.gllue.myproxy.constant.TimeConstants.NANOS_PER_SECOND;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.PlainFuture;
import com.gllue.myproxy.common.concurrent.SettableFuture;
import com.gllue.myproxy.transport.backend.command.CachedQueryResultReader;
import com.gllue.myproxy.transport.backend.command.CommandResultReader;
import com.gllue.myproxy.transport.backend.command.DefaultCommandResultReader;
import com.gllue.myproxy.transport.backend.datasource.DataSource;
import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.core.connection.AbstractConnection;
import com.gllue.myproxy.transport.core.connection.Connection;
import com.gllue.myproxy.transport.core.netty.NettyUtils;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.command.CommandPacket;
import com.gllue.myproxy.transport.protocol.packet.command.SimpleCommandPacket;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.prometheus.client.Summary;
import java.lang.ref.WeakReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackendConnectionImpl extends AbstractConnection implements BackendConnection {
  private enum State {
    INITIAL,
    ASSIGNED,
    RELEASED,
    CLOSED
  }

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

  private volatile State state = State.INITIAL;

  private WeakReference<DataSource<BackendConnection>> dataSourceRef;

  private volatile CommandResultReader commandResultReader;
  private volatile CommandResultReader newCommandResultReader;
  private volatile boolean firstResponse;
  private volatile long sendCommandTime;
  private long receiveFirstResponseTime;

  public BackendConnectionImpl(final int connectionId, final Channel channel) {
    super(connectionId, channel);
  }

  public BackendConnectionImpl(
      final int connectionId,
      final Channel channel,
      final DataSource<BackendConnection> dataSource) {
    this(connectionId, channel);
    setDataSource(dataSource);
  }

  @Override
  public void setDataSource(final DataSource<BackendConnection> dataSource) {
    this.dataSourceRef = new WeakReference<>(dataSource);
  }

  @Override
  public DataSource<BackendConnection> dataSource() {
    Preconditions.checkNotNull(dataSourceRef);
    var reference = dataSourceRef.get();
    if (reference == null) {
      throw new IllegalStateException("Data source was freed before connection closing.");
    }
    return reference;
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

    firstResponse = true;
    sendCommandTime = System.nanoTime();
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
    setCommandResultReader(reader);
    writeAndFlush(packet);
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
  public void assign() {
    assert dataSourceRef != null : "Does not set datasource on connection.";
    ensureStateNotClosed();

    assert state != State.ASSIGNED : "Backend connection is already assigned.";
    state = State.ASSIGNED;
  }

  @Override
  public boolean isAssigned() {
    return state == State.ASSIGNED;
  }

  @Override
  public boolean release() {
    assert dataSourceRef != null : "Does not set datasource on connection.";

    synchronized (this) {
      if (state != State.ASSIGNED) {
        log.warn("Connection is not assigned and the 'release' function cannot be invoked.");
        return false;
      }
      dataSource().releaseConnection(this);
      state = State.RELEASED;
    }
    return true;
  }

  @Override
  public boolean isReleased() {
    return state == State.RELEASED;
  }

  @Override
  public void releaseOrClose() {
    // If the command is being executed, we should close the backend connection
    // because the state is undefined. Otherwise the backend connection can be reused.
    if (commandResultReader != null) {
      close();
    } else {
      release();
    }
  }

  private void ensureStateNotClosed() {
    if (state == State.CLOSED) {
      throw new IllegalStateException("Illegal backend connection state [CLOSED].");
    }
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }

    synchronized (this) {
      if (state != State.CLOSED) {
        if (dataSourceRef != null) {
          dataSource().closeConnection(this);
        }
        NettyUtils.closeChannel(channel, false);
        state = State.CLOSED;
      }
    }
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED || super.isClosed();
  }

  @Override
  public void close(Consumer<Connection> onClosed) {
    if (state == State.CLOSED) {
      return;
    }

    synchronized (this) {
      if (state != State.CLOSED) {
        if (dataSourceRef != null) {
          dataSource().closeConnection(this);
        }
        NettyUtils.closeChannel(channel, (ignore) -> onClosed.accept(thisConnection()));
        state = State.CLOSED;
      }
    }
  }
}
