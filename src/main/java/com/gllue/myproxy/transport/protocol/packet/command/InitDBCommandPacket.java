package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Change the default schema of the connection
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-init-db.html#packet-COM_INIT_DB">COM_INIT_DB</a>
 */
public class InitDBCommandPacket extends AbstractCommandPacket {
  @Getter
  private final String schemaName;

  public InitDBCommandPacket(final String schemaName) {
    super(MySQLCommandPacketType.COM_INIT_DB);

    Preconditions.checkNotNull(schemaName);
    this.schemaName = schemaName;
  }

  public InitDBCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_INIT_DB);

    validateCommandType(payload);
    this.schemaName = payload.readStringEOF();
  }

  @Override
  public void write(MySQLPayload payload) {
    super.write(payload);
    payload.writeStringEOF(schemaName);
  }
}
