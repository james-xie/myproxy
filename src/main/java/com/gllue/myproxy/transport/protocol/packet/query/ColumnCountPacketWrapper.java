package com.gllue.myproxy.transport.protocol.packet.query;

import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.gllue.myproxy.transport.protocol.packet.AbstractPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;

public class ColumnCountPacketWrapper extends AbstractPacketWrapper {
  public ColumnCountPacketWrapper(final MySQLPacket packet) {
    super(packet);
  }

  public static MySQLPacket tryMatch(final MySQLPayload payload) {
    var packet = AbstractPacketWrapper.tryMatch(payload);
    if (packet != null) {
      return packet;
    } else if (OKPacket.match(payload)) {
      return new OKPacket(payload);
    }
    return new ColumnCountPacket(payload);
  }

  public static ColumnCountPacketWrapper newInstance(final MySQLPayload payload) {
    var packet = tryMatch(payload);
    return new ColumnCountPacketWrapper(packet);
  }

  public boolean isOkPacket() {
    return packet != null && packet instanceof OKPacket;
  }
}
