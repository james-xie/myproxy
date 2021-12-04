package com.gllue.transport.core.connection;

import com.gllue.common.concurrent.SettableFuture;
import com.gllue.transport.protocol.packet.MySQLPacket;
import java.net.SocketAddress;
import java.util.function.Consumer;

public interface Connection {
  int connectionId();

  SocketAddress localAddress();

  SocketAddress remoteAddress();

  void changeDatabase(String database);

  String currentDatabase();

  void write(MySQLPacket packet);

  void writeAndFlush(MySQLPacket packet);

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
