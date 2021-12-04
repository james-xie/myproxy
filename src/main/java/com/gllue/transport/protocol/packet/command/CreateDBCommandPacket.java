package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Create a schema
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-create-db.html#packet-COM_CREATE_DB">COM_CREATE_DB</a>
 */
public class CreateDBCommandPacket extends AbstractCommandPacket {
  @Getter
  private final String schemaName;

  public CreateDBCommandPacket(final String schemaName) {
    super(MySQLCommandPacketType.COM_CREATE_DB);

    Preconditions.checkNotNull(schemaName);
    this.schemaName = schemaName;
  }

  public CreateDBCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_CREATE_DB);

    validateCommandType(payload);
    this.schemaName = payload.readStringEOF();
  }

  @Override
  public void write(MySQLPayload payload) {
    super.write(payload);
    payload.writeStringEOF(schemaName);
  }
}
