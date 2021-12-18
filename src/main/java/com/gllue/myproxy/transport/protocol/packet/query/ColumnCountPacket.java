package com.gllue.myproxy.transport.protocol.packet.query;

import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import lombok.Getter;

@Getter
public class ColumnCountPacket implements MySQLPacket {
  private final int count;

  public ColumnCountPacket(final int count) {
    assert count > 0;
    this.count = count;
  }

  public ColumnCountPacket(final MySQLPayload payload) {
    this.count = (int)payload.readIntLenenc();
  }

  @Override
  public void write(MySQLPayload payload) {
    payload.writeIntLenenc(count);
  }
}
