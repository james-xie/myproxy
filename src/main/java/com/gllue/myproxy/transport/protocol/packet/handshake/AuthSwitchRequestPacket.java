package com.gllue.myproxy.transport.protocol.packet.handshake;

import com.gllue.myproxy.transport.exception.MalformedPacketException;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AuthSwitchRequestPacket implements MySQLPacket {
  public static final int HEADER = 0xfe;

  private final String pluginName;
  private final String authPluginData;

  public AuthSwitchRequestPacket(final MySQLPayload payload) {
    if (HEADER != payload.readInt1()) {
      throw new MalformedPacketException("Invalid auth switch response header");
    }
    pluginName = payload.readStringNul();
    authPluginData = payload.readStringEOF();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(HEADER);
    payload.writeStringNul(pluginName);
    payload.writeStringEOF(authPluginData);
  }

  public static boolean match(final MySQLPayload payload) {
    return payload.readableBytes() > 0 && payload.peek() == AuthSwitchRequestPacket.HEADER;
  }
}
