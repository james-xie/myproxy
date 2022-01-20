package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.SettableFuture;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import java.net.SocketAddress;
import java.util.function.Consumer;

public abstract class DelegateConnection implements Connection {
  protected final Connection connection;

  protected DelegateConnection(final Connection connection) {
    Preconditions.checkNotNull(connection, "Delegated connection cannot be null");
    this.connection = connection;
  }

  @Override
  public int connectionId() {
    return connection.connectionId();
  }

  @Override
  public String currentUser() {
    return connection.currentUser();
  }

  @Override
  public SocketAddress localAddress() {
    return connection.localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return connection.remoteAddress();
  }

  @Override
  public void changeDatabase(String database) {
    connection.changeDatabase(database);
  }

  @Override
  public String currentDatabase() {
    return connection.currentDatabase();
  }

  @Override
  public long createTime() {
    return connection.createTime();
  }

  @Override
  public long lastAccessTime() {
    return connection.lastAccessTime();
  }

  @Override
  public void begin() {
    connection.begin();
  }

  @Override
  public void commit() {
    connection.commit();
  }

  @Override
  public void rollback() {
    connection.rollback();
  }

  @Override
  public void enableAutoCommit() {
    connection.enableAutoCommit();
  }

  @Override
  public void disableAutoCommit() {
    connection.disableAutoCommit();
  }

  @Override
  public boolean isAutoCommit() {
    return connection.isAutoCommit();
  }

  @Override
  public boolean isTransactionOpened() {
    return connection.isTransactionOpened();
  }

  @Override
  public void write(MySQLPacket packet) {
    connection.write(packet);
  }

  @Override
  public void writeAndFlush(MySQLPacket packet) {
    connection.writeAndFlush(packet);
  }

  @Override
  public void write(MySQLPayload payload) {
    connection.write(payload);
  }

  @Override
  public void writeAndFlush(MySQLPayload payload) {
    connection.writeAndFlush(payload);
  }

  @Override
  public void write(MySQLPacket packet, SettableFuture<Connection> future) {
    connection.write(packet, future);
  }

  @Override
  public void writeAndFlush(MySQLPacket packet, SettableFuture<Connection> future) {
    connection.writeAndFlush(packet, future);
  }

  @Override
  public void flush() {
    connection.flush();
  }

  @Override
  public void onWritabilityChanged() {
    connection.onWritabilityChanged();
  }

  @Override
  public void addWritabilityChangedListener(WritabilityChangedListener<Connection> listener) {
    connection.addWritabilityChangedListener(listener);
  }

  @Override
  public void removeWritabilityChangedListener(WritabilityChangedListener<Connection> listener) {
    connection.removeWritabilityChangedListener(listener);
  }

  @Override
  public boolean isWritable() {
    return connection.isWritable();
  }

  @Override
  public boolean isAutoRead() {
    return connection.isAutoRead();
  }

  @Override
  public void enableAutoRead() {
    connection.enableAutoRead();
  }

  @Override
  public void disableAutoRead() {
    connection.disableAutoRead();
  }

  @Override
  public boolean isClosed() {
    return connection.isClosed();
  }

  @Override
  public void close() {
    connection.close();
  }

  @Override
  public void close(Consumer<Connection> onClosed) {
    connection.close(onClosed);
  }
}
