package com.gllue.myproxy.transport.protocol.packet;

public interface PacketWrapper {
  MySQLPacket getPacket();

  boolean isErrPacket();

  boolean isUnknownPacket();

  String getPacketDescription();
}
