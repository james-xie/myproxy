package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import java.util.HashSet;
import java.util.Set;

public class SimpleCommandPacket extends AbstractCommandPacket {
  public static final Set<MySQLCommandPacketType> COMMAND_TYPES = new HashSet<>();

  static {
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_SLEEP);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_QUIT);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_STATISTICS);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_PROCESS_INFO);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_CONNECT);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_DEBUG);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_PING);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_TIME);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_DELAYED_INSERT);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_CONNECT_OUT);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_DAEMON);
    COMMAND_TYPES.add(MySQLCommandPacketType.COM_RESET_CONNECTION);
  }

  public SimpleCommandPacket(final MySQLCommandPacketType commandType) {
    super(commandType);
    if (!COMMAND_TYPES.contains(commandType)) {
      throw new IllegalArgumentException(
          String.format("Illegal command type. [%s]", commandType.name()));
    }
  }
}
