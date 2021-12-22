package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;

public interface TrafficThrottlePipe extends AutoCloseable {
  void prepareToTransfer();

  void transfer(MySQLPacket packet, boolean forceFlush);
}
