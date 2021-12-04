package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.protocol.packet.MySQLPacket;

public interface CommandPacket extends MySQLPacket {
  MySQLCommandPacketType getCommandType();

  boolean databaseRequired();
}
