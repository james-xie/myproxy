package com.gllue.transport.protocol.packet.query;

import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Column Definition
 *
 * <pre>
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41">ColumnDefinition41</a>
 * </pre>
 */
@Getter
@AllArgsConstructor
public class ColumnDefinition41Packet implements MySQLPacket {
  private static final int LENGTH_OF_FIXED_LENGTH = 0x0c;

  private final String catalog;
  private final String schema;
  private final String table;
  private final String orgTable;
  private final String name;
  private final String orgName;
  private final int charset;
  private final int columnLength;
  private final int columnType;
  private final int flags;
  private final int decimals;
  private final String defaultValues;

  public ColumnDefinition41Packet(final MySQLPayload payload, final boolean isCommandFieldList) {
    catalog = payload.readStringLenenc();
    schema = payload.readStringLenenc();
    table = payload.readStringLenenc();
    orgTable = payload.readStringLenenc();
    name = payload.readStringLenenc();
    orgName = payload.readStringLenenc();
    var lengthOfFixedLength = payload.readIntLenenc();
    assert lengthOfFixedLength == LENGTH_OF_FIXED_LENGTH;
    charset = payload.readInt2();
    columnLength = payload.readInt4();
    columnType = payload.readInt1();
    flags = payload.readInt2();
    decimals = payload.readInt1();
    payload.skipBytes(2);
    if (isCommandFieldList) {
      defaultValues = payload.readStringFix((int) payload.readIntLenenc());
    } else {
      defaultValues = null;
    }
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeStringLenenc(catalog);
    payload.writeStringLenenc(schema);
    payload.writeStringLenenc(table);
    payload.writeStringLenenc(orgTable);
    payload.writeStringLenenc(name);
    payload.writeStringLenenc(orgName);
    payload.writeIntLenenc(LENGTH_OF_FIXED_LENGTH);
    payload.writeInt2(charset);
    payload.writeInt4(columnLength);
    payload.writeInt1(columnType);
    payload.writeInt2(flags);
    payload.writeInt1(decimals);
    payload.writeZero(2);
    if (defaultValues != null) {
      payload.writeIntLenenc(defaultValues.getBytes().length);
      payload.writeStringFix(defaultValues);
    }
  }
}
