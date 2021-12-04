package com.gllue.transport.core.connection;

import com.gllue.common.concurrent.AbstractRunnable;
import com.gllue.common.concurrent.SettableFuture;
import com.gllue.transport.core.netty.NettyUtils;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.channel.Channel;
import java.net.SocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractConnection implements Connection {
  protected final int connectionId;

  protected final Channel channel;

  private final Queue<WritabilityChangedListener<Connection>> writabilityChangedListeners;

  private volatile String currentDatabase;

  private volatile boolean active = true;

  public AbstractConnection(final int connectionId, final Channel channel) {
    this.connectionId = connectionId;
    this.channel = channel;
    writabilityChangedListeners = new LinkedTransferQueue<>();
  }

  @Override
  public SocketAddress localAddress() {
    return channel.localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return channel.remoteAddress();
  }

  @Override
  public void changeDatabase(String database) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(database));
    currentDatabase = database;
  }

  @Override
  public String currentDatabase() {
    return currentDatabase;
  }

  @Override
  public int connectionId() {
    return connectionId;
  }

  @Override
  public void write(final MySQLPacket packet) {
    channel.write(packet);
  }

  @Override
  public void writeAndFlush(final MySQLPacket packet) {
    channel.writeAndFlush(packet);
  }

  @Override
  public void write(final MySQLPacket packet, final SettableFuture<Connection> future) {
    channel
        .write(packet)
        .addListener(
            (f) -> {
              if (f.isCancelled()) {
                future.cancel(false);
              } else if (f.isSuccess()) {
                future.set(thisConnection());
              } else {
                future.setException(f.cause());
              }
            });
  }

  @Override
  public void writeAndFlush(final MySQLPacket packet, final SettableFuture<Connection> future) {
    channel
        .writeAndFlush(packet)
        .addListener(
            (f) -> {
              if (f.isCancelled()) {
                future.cancel(false);
              } else if (f.isSuccess()) {
                future.set(thisConnection());
              } else {
                future.setException(f.cause());
              }
            });
  }

  @Override
  public void flush() {
    channel.flush();
  }

  @Override
  public void addWritabilityChangedListener(WritabilityChangedListener<Connection> listener) {
    writabilityChangedListeners.offer(listener);
    // todo: remove this assertion.
    assert writabilityChangedListeners.size() < 10;
  }

  @Override
  public void removeWritabilityChangedListener(WritabilityChangedListener<Connection> listener) {
    writabilityChangedListeners.remove(listener);
  }

  @Override
  public void onWritabilityChanged() {
    if (writabilityChangedListeners.isEmpty()) {
      return;
    }

    for (var listener : writabilityChangedListeners) {
      listener
          .executor()
          .execute(
              new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                  listener.onSuccess(thisConnection());
                }

                @Override
                public void onFailure(Exception e) {
                  log.error("Failed to run writabilityChangedListener.", e);
                }
              });
    }
  }

  @Override
  public boolean isWritable() {
    return channel.isWritable();
  }

  @Override
  public boolean isAutoRead() {
    return channel.config().isAutoRead();
  }

  @Override
  public void enableAutoRead() {
    if (channel.eventLoop().inEventLoop()) {
      channel.config().setAutoRead(true);
    } else {
      channel
          .eventLoop()
          .execute(
              () -> {
                channel.config().setAutoRead(true);
              });
    }
  }

  @Override
  public void disableAutoRead() {
    if (channel.eventLoop().inEventLoop()) {
      channel.config().setAutoRead(false);
    } else {
      channel
          .eventLoop()
          .execute(
              () -> {
                channel.config().setAutoRead(false);
              });
    }
  }

  @Override
  public void close() {
    active = false;
    NettyUtils.closeChannel(channel, false);
  }

  @Override
  public boolean isClosed() {
    return !(active && channel.isActive());
  }

  @Override
  public void close(Consumer<Connection> onClosed) {
    active = false;
    NettyUtils.closeChannel(channel, (ignore) -> onClosed.accept(thisConnection()));
  }

  protected AbstractConnection thisConnection() {
    return this;
  }
}
