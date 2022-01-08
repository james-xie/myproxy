package com.gllue.myproxy.transport.protocol.packet.query;

import com.gllue.myproxy.transport.protocol.packet.AbstractPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;

public class TextResultSetRowPacketWrapper extends AbstractPacketWrapper {
  public TextResultSetRowPacketWrapper(final MySQLPacket packet) {
    super(packet);
  }

  public static MySQLPacket tryMatch(final MySQLPayload payload) {
    var packet = AbstractPacketWrapper.tryMatch(payload);
    if (packet != null) {
      return packet;
    } else if (EofPacket.match(payload)) {
      return new EofPacket(payload);
    } else if (OKPacket.match(payload)) {
      return new OKPacket(payload);
    }
    return null;
  }

  public static TextResultSetRowPacketWrapper newInstance(MySQLPayload payload, final int columns) {
    var packet = tryMatch(payload);
    if (packet == null) {
      packet = new TextResultSetRowPacket(payload, columns);
    }
    return new TextResultSetRowPacketWrapper(packet);
  }

  public boolean isEofPacket() {
    return packet != null && packet instanceof EofPacket;
  }

  public boolean isOkPacket() {
    return packet != null && packet instanceof OKPacket;
  }

}
