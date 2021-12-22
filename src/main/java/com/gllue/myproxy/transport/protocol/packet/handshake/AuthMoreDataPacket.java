package com.gllue.myproxy.transport.protocol.packet.handshake;

import com.gllue.myproxy.transport.exception.MalformedPacketException;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AuthMoreDataPacket implements MySQLPacket {
  public static final int HEADER = 0x01;

  private final String pluginData;

  public AuthMoreDataPacket(final MySQLPayload payload) {
    if (HEADER != payload.readInt1()) {
      throw new MalformedPacketException("Invalid auth more data header");
    }
    pluginData = payload.readStringEOF();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(HEADER);
    payload.writeStringEOF(pluginData);
  }

  public static boolean match(final MySQLPayload payload) {
    return payload.readableBytes() > 0 && payload.peek() == AuthMoreDataPacket.HEADER;
  }
}
