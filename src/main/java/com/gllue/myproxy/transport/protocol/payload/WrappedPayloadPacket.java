package com.gllue.myproxy.transport.protocol.payload;

import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class WrappedPayloadPacket implements MySQLPacket {
  private final MySQLPayload payload;

  @Override
  public void write(MySQLPayload payload) {
    throw new UnsupportedOperationException();
  }
}
