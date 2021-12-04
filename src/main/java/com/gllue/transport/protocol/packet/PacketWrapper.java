package com.gllue.transport.protocol.packet;

public interface PacketWrapper {
  MySQLPacket getPacket();

  boolean isErrPacket();

  boolean isUnknownPacket();

  String getPacketDescription();
}
