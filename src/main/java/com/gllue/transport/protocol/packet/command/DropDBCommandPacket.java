package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Drop a schema.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-drop-db.html#packet-COM_DROP_DB">COM_DROP_DB</a>
 */
public class DropDBCommandPacket extends AbstractCommandPacket {
  @Getter
  private final String schemaName;

  public DropDBCommandPacket(final String schemaName) {
    super(MySQLCommandPacketType.COM_DROP_DB);

    Preconditions.checkNotNull(schemaName);
    this.schemaName = schemaName;
  }

  public DropDBCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_DROP_DB);

    validateCommandType(payload);
    this.schemaName = payload.readStringEOF();
  }

  @Override
  public void write(MySQLPayload payload) {
    super.write(payload);
    payload.writeStringEOF(schemaName);
  }
}
