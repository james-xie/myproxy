package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * A COM_QUERY is used to send the server a text-based query that is executed immediately.
 *
 * @see <a
 *     href="https://dev.mysql.com/doc/internals/en/com-query.html#packet-COM_QUERY">COM_QUERY</a>
 */
public class QueryCommandPacket extends AbstractCommandPacket {
  public static final QueryCommandPacket BEGIN_COMMAND = new QueryCommandPacket("BEGIN");
  public static final QueryCommandPacket COMMIT_COMMAND = new QueryCommandPacket("COMMIT");
  public static final QueryCommandPacket ROLLBACK_COMMAND = new QueryCommandPacket("ROLLBACK");
  public static final QueryCommandPacket ENABLE_AUTO_COMMIT_COMMAND = new QueryCommandPacket("SET AUTOCOMMIT = true");
  public static final QueryCommandPacket DISABLE_AUTO_COMMIT_COMMAND = new QueryCommandPacket("SET AUTOCOMMIT = false");

  @Getter private final String query;

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
