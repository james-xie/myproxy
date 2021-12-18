package com.gllue.myproxy.transport.protocol.packet;

import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public abstract class AbstractPacketWrapper implements PacketWrapper {
  protected final MySQLPacket packet;

  @Nullable
  public static MySQLPacket tryMatch(final MySQLPayload payload) {
    if (ErrPacket.match(payload)) {
      return new ErrPacket(payload);
    }
    return null;
  }

  @Override
  public boolean isErrPacket() {
    return packet != null && packet instanceof ErrPacket;
  }

  @Override
  public boolean isUnknownPacket() {
    return packet == null;
  }

  @Override
  public String getPacketDescription() {
    if (packet == null) {
      return "Unknown packet";
    }
    return packet.toString();
  }
}
