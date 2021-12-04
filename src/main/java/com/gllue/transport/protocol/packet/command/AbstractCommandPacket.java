package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.exception.MalformedPacketException;
import com.gllue.transport.protocol.payload.MySQLPayload;

public abstract class AbstractCommandPacket implements CommandPacket {

  protected final MySQLCommandPacketType commandType;

  public AbstractCommandPacket(final MySQLCommandPacketType commandType) {
    this.commandType = commandType;
  }

  @Override
  public MySQLCommandPacketType getCommandType() {
    return this.commandType;
  }

  protected void validateCommandType(final MySQLPayload payload) {
    var cmdType = payload.readInt1();
    if (this.commandType.getValue() != cmdType) {
      throw new MalformedPacketException(
          String.format("Got an invalid command type.[%s]", cmdType));
    }
  }

  @Override
  public void write(MySQLPayload payload) {
    payload.writeInt1(this.commandType.getValue());
  }

  @Override
  public boolean databaseRequired() {
    return false;
  }
}
