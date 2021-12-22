package com.gllue.myproxy.transport.protocol.packet.query.text;

import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A row with the data for each column.
 *
 * <pre>
 * @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow">ResultsetRow</a>
 * </pre>
 */
@Getter
@RequiredArgsConstructor
public class TextResultSetRowPacket implements MySQLPacket {

  private final byte[][] rowData;

  public TextResultSetRowPacket(String[] rowData) {
    this.rowData = new byte[rowData.length][];
    for (int i=0; i<rowData.length; i++) {
      if (rowData[i] == null) {
        continue;
      }
      this.rowData[i] = rowData[i].getBytes();
    }
  }

  public TextResultSetRowPacket(final MySQLPayload payload, final int columns) {
    Preconditions.checkArgument(columns > 0, "Columns must be greater than 0");

    rowData = new byte[columns][];
    for (int i=0; i<columns; i++) {
      if (payload.peek() == NULL) {
        payload.readInt1();
        rowData[i] = null;
      } else {
        rowData[i] = payload.readStringLenencReturnBytes();
      }
    }
  }

  @Override
  public void write(final MySQLPayload payload) {
    for (var columnData: rowData) {
      if (columnData == null) {
        payload.writeInt1(NULL);
      } else {
        payload.writeBytesLenenc(columnData);
      }
    }
  }
}
