package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.AbstractRunnable;
import com.gllue.myproxy.common.concurrent.SettableFuture;
import com.gllue.myproxy.transport.core.netty.NettyUtils;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractConnection implements Connection {
  protected final int connectionId;

  protected final String user;

  protected final Channel channel;

  private final Queue<WritabilityChangedListener<Connection>> writabilityChangedListeners;

  private volatile String currentDatabase;

  private volatile boolean active = true;

  private volatile boolean transactionOpened;

  private volatile boolean autoCommit = true;

  private final long createTime;

  private long lastAccessTime;

  public AbstractConnection(final int connectionId, final String user, final Channel channel) {
    this.connectionId = connectionId;
    this.user = user;
    this.channel = channel;
    this.writabilityChangedListeners = new LinkedTransferQueue<>();
    this.createTime = System.currentTimeMillis();
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
  public String currentUser() {
    return user;
  }

  @Override
  public long createTime() {
    return createTime;
  }

  @Override
  public long lastAccessTime() {
    return lastAccessTime;
  }

  protected void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  @Override
  public void begin() {
    transactionOpened = true;
  }

  @Override
  public void commit() {
    transactionOpened = false;
  }

  @Override
  public void rollback() {
    transactionOpened = false;
  }

  @Override
  public boolean isTransactionOpened() {
    return transactionOpened || !autoCommit;
  }

  @Override
  public void enableAutoCommit() {
    autoCommit = true;
  }

  @Override
  public void disableAutoCommit() {
    autoCommit = false;
  }

  @Override
  public boolean isAutoCommit() {
    return autoCommit;
  }

  private ChannelFuture doChannelWrite(Object msg, boolean flush) {
    if (!flush) {
      return channel.write(msg);
    } else {
      return channel.writeAndFlush(msg);
    }
  }

  @Override
  public void write(final MySQLPacket packet) {
    doChannelWrite(packet, false);
  }

  @Override
  public void writeAndFlush(final MySQLPacket packet) {
    doChannelWrite(packet, true);
  }

  @Override
  public void write(MySQLPayload payload) {
    doChannelWrite(payload, false);
  }

  @Override
  public void writeAndFlush(MySQLPayload payload) {
    doChannelWrite(payload, true);
  }

  @Override
  public void write(final MySQLPacket packet, final SettableFuture<Connection> future) {
    doChannelWrite(packet, false)
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
    doChannelWrite(packet, true)
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

  protected void onClosed() {}

  @Override
  public void close() {
    close((c) -> {});
  }

  @Override
  public boolean isClosed() {
    return !(active && channel.isActive());
  }

  @Override
  public void close(Consumer<Connection> onClosed) {
    active = false;
    NettyUtils.closeChannel(channel, (ignore) -> onClosed.accept(thisConnection()));
    onClosed();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractConnection that = (AbstractConnection) o;
    return connectionId == that.connectionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionId);
  }

  protected AbstractConnection thisConnection() {
    return this;
  }
}
