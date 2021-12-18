package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;

public interface CommandPacket extends MySQLPacket {
  MySQLCommandPacketType getCommandType();

  boolean databaseRequired();
}
