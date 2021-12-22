package com.gllue.myproxy.transport.protocol.packet.handshake;

import com.gllue.myproxy.transport.protocol.packet.AbstractPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;

public class AuthPacketWrapper extends AbstractPacketWrapper {
  public AuthPacketWrapper(MySQLPacket packet) {
    super(packet);
  }

  public static MySQLPacket tryMatch(final MySQLPayload payload) {
    var packet = AbstractPacketWrapper.tryMatch(payload);
    if (packet != null) {
      return packet;
    }

    if (AuthSwitchRequestPacket.match(payload)) {
      packet = new AuthSwitchRequestPacket(payload);
    } else if (AuthMoreDataPacket.match(payload)) {
      packet = new AuthMoreDataPacket(payload);
    } else if (OKPacket.match(payload)) {
      packet = new OKPacket(payload);
    }
    return packet;
  }

  public static AuthPacketWrapper newInstance(final MySQLPayload payload) {
    var packet = tryMatch(payload);
    return new AuthPacketWrapper(packet);
  }

  public boolean isOkPacket() {
    return packet != null && packet instanceof OKPacket;
  }

  public boolean isAuthSwitchPacket() {
    return packet != null && packet instanceof AuthSwitchRequestPacket;
  }

  public boolean isAuthMoreDataPacket() {
    return packet != null && packet instanceof AuthMoreDataPacket;
  }
}
