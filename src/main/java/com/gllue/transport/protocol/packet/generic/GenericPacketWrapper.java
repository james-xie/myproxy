package com.gllue.transport.protocol.packet.generic;

import com.gllue.transport.protocol.packet.AbstractPacketWrapper;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import javax.annotation.Nullable;
import lombok.Getter;

@Getter
public class GenericPacketWrapper extends AbstractPacketWrapper {

  public GenericPacketWrapper(final MySQLPacket packet) {
    super(packet);
  }

  @Nullable
  public static MySQLPacket tryMatch(final MySQLPayload payload) {
    MySQLPacket packet;
    if (OKPacket.match(payload)) {
      packet = new OKPacket(payload);
    } else if (EofPacket.match(payload)) {
      packet = new EofPacket(payload);
    } else {
      packet = AbstractPacketWrapper.tryMatch(payload);
    }
    return packet;
  }

  public static GenericPacketWrapper newInstance(MySQLPayload payload) {
    var packet = tryMatch(payload);
    return new GenericPacketWrapper(packet);
  }

  public boolean isOkPacket() {
    return packet != null && packet instanceof OKPacket;
  }

  public boolean isEofPacket() {
    return packet != null && packet instanceof EofPacket;
  }
}
