package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Get the column definitions of a table.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-field-list.html#packet-COM_FIELD_LIST">COM_FIELD_LIST</a>
 */
public class FieldListCommandPacket extends AbstractCommandPacket {
  @Getter
  private final String query;

  public FieldListCommandPacket(final String query) {
    super(MySQLCommandPacketType.COM_FIELD_LIST);

    Preconditions.checkNotNull(query);
    this.query = query;
  }

  public FieldListCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_FIELD_LIST);

    validateCommandType(payload);
    this.query = payload.readStringEOF();
  }

  @Override
  public void write(MySQLPayload payload) {
    super.write(payload);
    payload.writeStringEOF(query);
  }

  @Override
  public boolean databaseRequired() {
    return true;
  }
}
