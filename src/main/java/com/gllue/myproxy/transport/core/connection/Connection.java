package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.concurrent.SettableFuture;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import java.net.SocketAddress;
import java.util.function.Consumer;

public interface Connection {
  int connectionId();

  String currentUser();

  SocketAddress localAddress();

  SocketAddress remoteAddress();

  void changeDatabase(String database);

  String currentDatabase();

  void begin();

  void commit();

  void rollback();

  void enableAutoCommit();

  void disableAutoCommit();

  boolean isAutoCommit();

  boolean isTransactionOpened();

  void write(MySQLPacket packet);

  void writeAndFlush(MySQLPacket packet);

  void write(MySQLPayload payload);

  void writeAndFlush(MySQLPayload payload);

  void write(MySQLPacket packet, SettableFuture<Connection> future);

  void writeAndFlush(MySQLPacket packet, SettableFuture<Connection> future);

  void flush();

  void onWritabilityChanged();

  void addWritabilityChangedListener(WritabilityChangedListener<Connection> listener);

  void removeWritabilityChangedListener(WritabilityChangedListener<Connection> listener);

  boolean isWritable();

  boolean isAutoRead();

  void enableAutoRead();

  void disableAutoRead();

  boolean isClosed();

  void close();

  void close(Consumer<Connection> onClosed);
}
