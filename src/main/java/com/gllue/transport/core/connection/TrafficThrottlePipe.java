package com.gllue.transport.core.connection;

import com.gllue.transport.protocol.packet.MySQLPacket;

public interface TrafficThrottlePipe extends AutoCloseable {
  void prepareToTransfer();

  void transfer(MySQLPacket packet, boolean forceFlush);
}
