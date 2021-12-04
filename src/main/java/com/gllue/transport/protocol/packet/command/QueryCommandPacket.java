package com.gllue.transport.protocol.packet.command;

import com.gllue.transport.constant.MySQLCommandPacketType;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * A COM_QUERY is used to send the server a text-based query that is executed immediately.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-query.html#packet-COM_QUERY">COM_QUERY</a>
 */
public class QueryCommandPacket extends AbstractCommandPacket {
  @Getter
  private final String query;

  public QueryCommandPacket(final String query) {
    super(MySQLCommandPacketType.COM_QUERY);

    Preconditions.checkNotNull(query);
    this.query = query;
  }

  public QueryCommandPacket(final MySQLPayload payload) {
    super(MySQLCommandPacketType.COM_QUERY);

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
