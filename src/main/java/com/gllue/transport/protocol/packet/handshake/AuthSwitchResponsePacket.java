package com.gllue.transport.protocol.packet.handshake;

import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AuthSwitchResponsePacket implements MySQLPacket {

  private final byte[] authPluginData;

  public AuthSwitchResponsePacket(final MySQLPayload payload) {
    authPluginData = payload.readStringEOFReturnBytes();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeBytes(authPluginData);
  }
}
